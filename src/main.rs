/*
Trading backtesting platform (currently only generates Renko bars from tick data)

- algo message queue & consumer thread
- market data message queue & consumer thread
  - tick data is processed into bars
    - support time-based (seconds, minutes, hours, days), # ticks, P&F, Kagi, Renko, Range, Kase, Heikin-Ashi)
    - as bars are completed, send message to algo to calculate indicators & execute strategies
- separate producer thread for pub/sub IQFeed
  - inject data into market data message queue)
- separate producer thread for tick playback
  - inject data into market data message queue based on playback speed settings
*/
extern crate rusqlite;
extern crate chrono;

use std::collections::VecDeque;
use std::env;
use std::error::Error;

//use chrono::prelude::*;
use chrono::{Duration, Local, NaiveDateTime, Timelike};  // Duration, NaiveTime, Timelike
//use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use rusqlite::{params, Connection, Result};
use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef, FromSqlError};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde_json;

#[derive(Debug)]
struct Tick {
  date_time: NaiveDateTime, //DateTime<Utc>,
  last: Decimal,  // f64,
  last_sz: u64,
  /*
  last_type: String,
  mkt_ctr: i32,
  tot_vlm: i32,
  */
  bid: Decimal,  // f64,
  ask: Decimal,  // f64,
  /*
  cond1: i32,
  cond2: i32,
  cond3: i32,
  cond4: i32,
  agg: i32,
  day: i32,
  */
}

/*
struct BidAskVol {
  bid_vol: i64,
  ask_vol: i64,
}
*/

static DEBUG:bool = false;

#[derive(Debug)]
struct Bar {
  date_time: NaiveDateTime, //DateTime<Utc>,
  open: Decimal,   // f64,
  high: Decimal,   // f64,
  low: Decimal,    // f64,
  close: Decimal,  // f64,
  up: i32,
  down: i32,
  tick: u64,
  volume: u64,
  volume_bid: VecDeque<u64>,
  volume_ask: VecDeque<u64>
}

pub fn round_with_precision(value: f64, precision: u32) -> f64
{
  let multiplier = 10f64.powi(precision as i32);
  let x:f64 = (value * multiplier).round() / multiplier;
  println!("x={}", x);
  x
}

pub struct MyDecimal(Decimal);

impl MyDecimal {
  pub fn new(value: Decimal) -> Self {
    MyDecimal(value)
  }

  pub fn inner(&self) -> &Decimal {
    &self.0
  }

  pub fn into_inner(self) -> Decimal {
    self.0
  }
}

impl From<Decimal> for MyDecimal {
  fn from(decimal: Decimal) -> Self {
    MyDecimal(decimal)
  }
}

impl Into<Decimal> for MyDecimal {
  fn into(self) -> Decimal {
    self.0
  }
}

impl ToSql for MyDecimal {
  fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
    Ok(ToSqlOutput::Owned(self.0.to_string().into()))
  }
}

/*
impl FromSql for MyDecimal {
  fn column_result(value: ValueRef) -> FromSqlResult<Self> {
      value.as_str().and_then(|str| Decimal::from_str(str).map(MyDecimal).map_err(|e| FromSqlError::Other(Box::new(e))))
  }
}
*/
/*
impl FromSql for MyDecimal {
  fn column_result(value: ValueRef) -> rusqlite::types::FromSqlResult<Self> {
    value.as_str().and_then(|str| {
        Decimal::from_str(str).map(MyDecimal).map_err(|e| rusqlite::types::FromSqlError::Other(Box::new(e)))
    })
  }
}
*/

impl FromSql for MyDecimal {
  fn column_result(value: ValueRef) -> FromSqlResult<Self> {
    match value {
      ValueRef::Real(f) => Decimal::from_f64(f)
        .map(MyDecimal)
        .ok_or(FromSqlError::InvalidType),
      _ => Err(FromSqlError::InvalidType),
    }
  }
}

fn read_ticks(conn: &Connection) -> Result<Vec<Tick>>
{
  let mut start_time = Local::now();

  //let mut stmt = conn.prepare("SELECT date, time, last, last_sz, last_type, mkt_ctr, tot_vlm, bid, ask, cond1, cond2, cond3, cond4, agg, day FROM ticks")?;
  //let mut stmt = conn.prepare("SELECT date, time, last, last_sz, last_type, mkt_ctr, tot_vlm, bid, ask, cond1, cond2, cond3, cond4, agg, day FROM ticks")?;
  let mut stmt = conn.prepare("SELECT date, time, last, last_sz, bid, ask FROM ticks")?;
  let tick_iter = stmt.query_map([], |row| {

    let date_time_str: String = row.get(0)?;
    let date_str = date_time_str.split_whitespace().next().ok_or_else(|| {
      rusqlite::Error::InvalidColumnType(0, "date".to_string(), rusqlite::types::Type::Text)
    })?;
    let time: String = row.get(1)?;
    //println!("date_time_str: {} date: {} time: {}", date_time_str, date_str, time);
    let date_time: NaiveDateTime = match NaiveDateTime::parse_from_str(&format!("{} {}", date_str, time), "%Y-%m-%d %H:%M:%S%.f") {
      Ok(dt) => dt,
      Err(e) => {
        eprintln!("Error parsing date and time: {}", e);
        return Err(rusqlite::Error::ExecuteReturnedResults);
      }
    };
    //println!("date: {} time: {} date_time: {:?}", date_str, time, date_time);

    Ok(Tick {
      date_time,
      //last: row.get(2)?,
      last: row.get::<_, MyDecimal>(2)?.0,
      last_sz: row.get(3)?,
      /*
      last_type: "C".to_string(), //row.get(4)?,
      mkt_ctr: row.get(5)?,
      tot_vlm: row.get(6)?,
      */
      bid: row.get::<_, MyDecimal>(4)?.0,
      ask: row.get::<_, MyDecimal>(5)?.0,
      /*
      cond1: row.get(9)?,
      cond2: row.get(10)?,
      cond3: row.get(11)?,
      cond4: row.get(12)?,
      agg: row.get(13)?,
      day: row.get(14)?,
      */
    })
  })?;

  let mut delta_time = Local::now() - start_time;
  if DEBUG { println!("SELECT elapsed {:.3}ms", delta_time.num_milliseconds() as f64); }

  start_time = Local::now();

  let mut ticks = Vec::new();
  for tick in tick_iter {
    //println!("TICK: {:?}", tick);
    ticks.push(tick?);
  }
  delta_time = Local::now() - start_time;
  if DEBUG {
    println!("Vec push() elapsed {:.3}ms", delta_time.num_milliseconds() as f64);
    println!("ticks.len(): {}", ticks.len());
  }

  Ok(ticks)
}

fn update_deque(
  tick: &Tick,
  bid_ask_high: &mut Decimal,  // f64,
  bid_ask_low: &mut Decimal,  // f64,
  tick_size: Decimal,  // f64,
  max_size: u32,
  volume_bid: &mut VecDeque<u64>,
  volume_ask: &mut VecDeque<u64>,
) {
  if DEBUG { println!("update_deque: bid_ask_high={:?} bid_ask_low={:?} last={:?}", bid_ask_high, bid_ask_low, tick.last); }

  let get_index = |price: Decimal, base_price: Decimal| -> usize {
    // Calculate the difference and divide by the tick size
    let decimal_result = (price - base_price) / tick_size;

    // Round the decimal result
    let rounded_result = decimal_result.round();

    // Convert the Decimal to f64, handle potential failure
    let float_result = rounded_result.to_f64().unwrap_or(0.0);

    // Cast f64 to usize
    float_result as usize
  };

  let time = tick.date_time.time();

  if volume_bid.is_empty() || volume_ask.is_empty() {
    volume_bid.resize(1, 0);
    volume_ask.resize(1, 0);
    *bid_ask_high = tick.last;
    *bid_ask_low = tick.last;
    if DEBUG && time.hour() < 2 { println!("UC1 bid_ask_high={} bid_ask_low={}", *bid_ask_high, *bid_ask_low); }
  } else {
    if volume_bid.len() < max_size as usize {
      if DEBUG && time.hour() < 2 { println!("UC2a bid_ask_high={} bid_ask_low={}", *bid_ask_high, *bid_ask_low); }
    } else {
      if DEBUG && time.hour() < 2 { println!("UC2b bid_ask_high={} bid_ask_low={}", *bid_ask_high, *bid_ask_low); }
    }
  }

  if DEBUG && time.hour() < 2 { println!("BEFORE {:?} bid_ask_low={} bid_ask_high={} bid={:?} ask={:?}", tick, *bid_ask_low, *bid_ask_high, volume_bid, volume_ask); }

  // Adjust deques if the new price is outside the current range
  while tick.last < *bid_ask_low {
    volume_bid.push_front(0);
    volume_ask.push_front(0);
    if DEBUG && time.hour() < 2 { println!("LOW push len={} last={} high={} bid={:?} ask={:?}", volume_bid.len(), tick.last, *bid_ask_high, volume_bid, volume_ask); }
    if volume_bid.len() > max_size as usize {
      volume_bid.pop_back();
      println!("WTF BID max!!!");
    }
    if volume_ask.len() > max_size as usize {
      volume_ask.pop_back();
      println!("WTF ASK max!!!");
    }
    *bid_ask_low -= tick_size;
  }
  while tick.last > *bid_ask_high {
    volume_bid.push_back(0);
    volume_ask.push_back(0);
    if DEBUG && time.hour() < 2 { println!("HIGH push len={} last={} high={} bid={:?} ask={:?}", volume_bid.len(), tick.last, *bid_ask_high, volume_bid, volume_ask); }
    if volume_bid.len() > max_size as usize {
      volume_bid.pop_front();
      println!("WTF BID max!!!");
    }
    if volume_ask.len() > max_size as usize {
      volume_ask.pop_front();
      println!("WTF ASK max!!!");
    }
    *bid_ask_high += tick_size;
  }

  // Update bis/ask volumes
  if tick.last >= *bid_ask_low && tick.last <= *bid_ask_high {
    let bid_ask_index = get_index(tick.last, *bid_ask_low);

    if tick.last <= tick.bid {
      if bid_ask_index < volume_bid.len() {
        volume_bid[bid_ask_index] += tick.last_sz;
      } else {
        println!("WTF BID!!! {} {} {} {} {}", tick.last, *bid_ask_low, *bid_ask_high, bid_ask_index, volume_bid.len());
      }
    }

    if tick.last >= tick.ask {
      if bid_ask_index < volume_ask.len() {
        volume_ask[bid_ask_index] += tick.last_sz;
      } else {
        println!("WTF ASK!!! {} {} {} {} {}", tick.last, *bid_ask_low, *bid_ask_high, bid_ask_index, volume_ask.len());
      }
    }
  } else {
    println!("WTF outside current!!! {} {} {}", tick.last, *bid_ask_low, *bid_ask_high);
  }

  if time.hour() < 2 {
    if DEBUG && time.hour() < 2 { println!("AFTER {:?} bid_ask_low={} bid_ask_high={} bid={:?} ask={:?}", tick, *bid_ask_low, *bid_ask_high, volume_bid, volume_ask); }
  }
}

fn generate_renko_bars(ticks: Vec<Tick>, bar_interval: u32, tick_size: Decimal) -> Vec<Bar>
{
  let mut bars = Vec::new();
  if ticks.is_empty() {
    return bars;
  }
  let ticks_len = ticks.len();

  let start_time = Local::now();

  let bar_height = Decimal::from(bar_interval) * tick_size;
  let mut last_open = ticks[0].last;
  let mut last_close = ticks[0].last;
  let mut last_tick_price = ticks[0].last;
  let mut direction = 1; // 0 for no direction, 1 for up, -1 for down
  //let mut last_direction: i32 = 0;
  let mut up_count = 0;
  let mut down_count = 0;
  let mut bid_ask_high = Decimal::ZERO;
  let mut bid_ask_low = Decimal::ZERO;
  let mut tick_counter = 1;
  let num_ticks = ticks.len();
  let mut tick_jump: Decimal;
  let max_size: u32 = bar_interval * 3;
  let mut volume: u64 = 0;
  let mut volume_bid: VecDeque<u64> = VecDeque::with_capacity(max_size as usize);
  let mut volume_ask: VecDeque<u64> = VecDeque::with_capacity(max_size as usize);

  // let mut bid_ask_vol: VecDeque<(f) = VecDeque::new();
  // let mut vol_bid = VecDeque::new();

  for mut tick in ticks {

    // use cases - ticks are either:
    // 1) first tick
    // 2) inter-bar
    // 2a) incomplete bar (bid/ask volume applied to current)
    // 2b) precisely completes bar (bid/ask volume applied to current)
    // 3) completes bar past end of current bar (end of current bar padded as necessary, current bid/ask volume applied to next)

    if tick.last > last_tick_price {
      direction = 1;
      up_count += 1;
    } else if tick.last < last_tick_price {
      direction = -1;
      down_count += 1;
    } else {
      // Price stayed the same, increment the count based on last direction
      if direction == 1 {
        up_count += 1;
      } else if direction == -1 {
        down_count += 1;
      } else {
        up_count = 1;  // first tick assumed "up"
        direction = 1;
      }
    }

    let price_diff: Decimal;
    let prior_up: bool = last_open <= last_close;
    if prior_up {  // first or prior up
      if tick.last >= last_close {  // price above prior bar; UU
        price_diff = tick.last - last_close;
      } else {  // reversal down; UD
        price_diff = last_open - tick.last;
      }
    } else  {  // prior down
      if tick.last > last_open {  // reversal up; DU
        price_diff = tick.last - last_open;
      } else  {  // DD
        price_diff = last_close - tick.last;
      }
    }

    if price_diff <= bar_height {  // still inter-bar; no phantom (use cases 1, 2a & 2b)
      update_deque(&tick, &mut bid_ask_high, &mut bid_ask_low, tick_size, max_size, &mut volume_bid, &mut volume_ask);
    }

    //println!(
    //  "tick: {:>5} tick.last: {:>8.2} last_open: {:>8.2} last_close: {:>8.2} diff: {:>5.2} high: {:>5.2} low: {:>5.2} dir: {:>2}",
    //  tick_counter, tick.last, last_open, last_close, price_diff, bid_ask_high, bid_ask_low, direction
    //);

    if price_diff >= bar_height || tick_counter == num_ticks {  // completed a bar or final tick

      //last_direction = direction;

      let mut reset_vol = false;
      if price_diff == bar_height || tick_counter == num_ticks {
        // apply volume to current bar (not next)
        volume += tick.last_sz;
        reset_vol = true;
      }

      // Calculate new bar
      let new_open: Decimal;
      let new_close: Decimal;

      if price_diff >= bar_height {
        if direction == 1 {
          if prior_up {  // UU
            new_open = last_close;
          } else {  // DU
            new_open = last_open;
          }
          new_close = new_open + bar_height;
          bid_ask_high = new_close;  // could be higher if final bar?
        } else {
          if prior_up {  // UD
            new_open = last_open;
          } else {  // DD
            new_open = last_close;
          }
          new_close = new_open - bar_height;
          bid_ask_low = new_close;  // could be lower if final bar?
        }
      } else {  // final tick for the day; are we above, below or in the middle of the bar?
        let last_high = last_open.max(last_close);  // TODO: use bars.last()?
        let last_low = last_open.min(last_close);
        if tick.last >= last_high {
          new_open = last_high;
        } else if tick.last <= last_low {
          new_open = last_low;
        } else {
          new_open = tick.last;
        }
        new_close = tick.last;
      }

      if let Some(last_bar) = bars.last() {
        if last_bar.date_time >= tick.date_time {
          //println!("{} BEFORE {}", tick_counter, tick.date_time);
          tick.date_time = last_bar.date_time.checked_add_signed(Duration::microseconds(1))
            .expect("Overflow occurred");
          //println!("{} AFTER {}", tick_counter, tick.date_time);
        }
      }

      if bid_ask_low == Decimal::ZERO {
        bid_ask_low = new_open;
      }

      if bid_ask_high == Decimal::ZERO {
        bid_ask_high = new_open;
      }

      let bar = Bar {
        date_time: tick.date_time,
        open: new_open,
        high: bid_ask_high,
        low: bid_ask_low,
        close: new_close,
        up: up_count,
        down: down_count,
        tick: tick_counter as u64,
        volume: volume,
        volume_bid: volume_bid,
        volume_ask: volume_ask
      };

      if reset_vol {
        volume = 0;
      } else {
        volume = tick.last_sz;
      }

      //println!("BAR {} O: {:>8.2} H: {:>8.2} L: {:>8.2} C: {:>8.2} U: {:>5} D: {:>5} T: {:>6}", bars.len() + 1, bar.open, bar.high, bar.low, bar.close, bar.up, bar.down, bar.tick);

      bars.push(bar);

      // Update last_close for next bar
      tick_jump = price_diff / bar_height;
      if tick_jump >= dec!(2.0) {
        // println!("PHANTOM tick_jump: {:.2}", tick_jump);
        if direction == 1 {
          bid_ask_high = tick.last;
          bid_ask_low = new_close + bar_height * (tick_jump.floor() - dec!(1.0));
          last_open = bid_ask_low;
          last_close = bid_ask_low;
        } else {
          bid_ask_low = tick.last;
          bid_ask_high = new_close - bar_height * (tick_jump.floor() - dec!(1.0));
          last_open = bid_ask_high;
          last_close = bid_ask_high;
        }
      } else {
        last_open = new_open;
        last_close = new_close;
        bid_ask_high = Decimal::ZERO;
        bid_ask_low = Decimal::ZERO;
      }

      volume_bid = VecDeque::with_capacity(max_size as usize);
      volume_ask = VecDeque::with_capacity(max_size as usize);
      if !reset_vol {
        let time = tick.date_time.time();
        if DEBUG && time.hour() < 2 { println!("UC3 bid_ask_high={} bid_ask_low={}", bid_ask_high, bid_ask_low); }
        update_deque(&tick, &mut bid_ask_high, &mut bid_ask_low, tick_size, max_size, &mut volume_bid, &mut volume_ask);
      }

      up_count = 0;
      down_count = 0;
    } else {
      volume += tick.last_sz;
    }
    last_tick_price = tick.last;
    tick_counter += 1;
  }
  let delta_time = Local::now() - start_time;
  println!("{} ticks {} bars elapsed {:.3}ms", ticks_len, bars.len(), delta_time.num_milliseconds() as f64);
  //println!("{:#?}", bars);

  bars
}

fn write_bars(conn: &mut Connection, bars: Vec<Bar>, bar_interval: u32) -> Result<()>
{
  /*
  let pb = ProgressBar::new(bars.len() as u64);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));
  */

  let tx = conn.transaction()?;
  let start_time = Local::now();

  {
    let mut stmt = tx.prepare(
      &format!("INSERT INTO bars_renko_{} (date_time, open, high, low, close, up, down, tick, volume, volume_bid, volume_ask) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)", bar_interval),
    )?;

    //let mut bar_num: u64 = 0;
    for bar in bars {
      //conn.execute(
      //  "INSERT INTO bars (Date_Time, Open, High, Low, Close, Up, Down, Tick) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
      //  params![bar.date_time.format("%Y-%m-%d %H:%M:%S%.6f").to_string(), bar.open, bar.high, bar.low, bar.close, bar.up, bar.down, bar.tick],
      //  //params![bar.date_time, bar.open, bar.high, bar.low, bar.close, bar.up, bar.down, bar.tick],
      //)?;
      let serialized_bid = serde_json::to_string(&bar.volume_bid).unwrap();
      let serialized_ask = serde_json::to_string(&bar.volume_ask).unwrap();
      stmt.execute(params![
        bar.date_time.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        MyDecimal::new(bar.open),
        MyDecimal::new(bar.high),
        MyDecimal::new(bar.low),
        MyDecimal::new(bar.close),
        bar.up,
        bar.down,
        bar.tick,
        bar.volume,
        serialized_bid,
        serialized_ask
      ])?;
      //bar_num += 1;
      //pb.set_position(bar_num);
    }
  }

  tx.commit()?;

  let delta_time = Local::now() - start_time;
  if DEBUG { println!("write_bars() elapsed {:.3}ms", delta_time.num_milliseconds() as f64); }
  Ok(())
}

fn set_wal_mode(conn: &mut Connection) -> Result<()> {
  let mut stmt = conn.prepare("PRAGMA journal_mode = WAL;")?;
  let _journal_mode: String = stmt.query_row([], |row| row.get(0))?;

  //println!("Journal mode set to: {}", _journal_mode);
  Ok(())
}

fn set_synchronous_mode(conn: &mut Connection) -> Result<()> {
  conn.execute("PRAGMA synchronous = NORMAL;", [])?;

  //println!("Synchronous mode set to NORMAL");
  Ok(())
}

fn setup_database(conn: &Connection, bar_interval: u32) -> Result<()>
{
  conn.execute(&format!("DROP TABLE IF EXISTS bars_renko_{}", bar_interval), [])?;

  conn.execute(
    &format!("CREATE TABLE bars_renko_{} (
      date_time TEXT,
      open REAL,
      high REAL,
      low REAL,
      close REAL,
      up INTEGER,
      down INTEGER,
      tick INTEGER,
      volume INTEGER,
      volume_bid TEXT,
      volume_ask TEXT
    )", bar_interval), [],
  )?;

  Ok(())
}
//fn main() -> Result<()> // std::io::Result<()>
fn main() -> Result<(), Box<dyn Error>>
{
  let binding: String;
  let db_file_path: &String;
  let bar_interval: u32;
  let tick_size: Decimal;  // f64;

  let args: Vec<String> = env::args().collect();

  if args.len() < 4 {
    // return Err("Usage: renko <path_to_your_db.db> <bar interval> <tick size>".into());
    binding = "D:\\db\\IQFeed\\SQLite\\NQM5\\NQM5_20250514.db".to_string();
    db_file_path = &binding;
    bar_interval = 8;
    tick_size = dec!(0.25);
  } else {
    db_file_path = &args[1];
    bar_interval = args[2].parse()?;
    tick_size = args[3].parse()?;
  }

  if DEBUG {
    println!("\nARGS=[");
    args.iter().for_each(|arg| println!("{}", arg));
    println!("\ndb_file_path={db_file_path}\n");
  }

  let mut conn = Connection::open(db_file_path)?;

  set_wal_mode(&mut conn)?;
  set_synchronous_mode(&mut conn)?;

  setup_database(&conn, bar_interval)?;

  let ticks = read_ticks(&conn)?;
  let bars = generate_renko_bars(ticks, bar_interval, tick_size);
  write_bars(&mut conn, bars, bar_interval)?;

  Ok(())
}