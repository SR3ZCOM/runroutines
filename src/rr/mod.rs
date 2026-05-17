pub mod runroutines;

pub const NC: &str = "\x1b[0m"; // NO_COLOR
pub const DRED: &str = "\x1b[31m";
pub const RED: &str = "\x1b[91m";
pub const DGREEN: &str = "\x1b[32m";
pub const GREEN: &str = "\x1b[92m";
pub const DYELLOW: &str = "\x1b[33m";
pub const YELLOW: &str = "\x1b[93m";
pub const MAG: &str = "\x1b[95m";
pub const DCYAN: &str = "\x1b[36m";
pub const CYAN: &str = "\x1b[96m";

#[macro_export]
macro_rules! seprint_r {
  ($fmt:literal) => {
    eprintln!( concat!("❌ {}{}: ", $fmt, "{}"), RED, LOG_VALUE, NC );
  };

  ($fmt:literal $(, $args:expr)*) => {
    eprintln!( concat!("❌ {}{}: ", $fmt, "{}"), RED, LOG_VALUE, $($args),*, NC );
  };
}

#[macro_export]
macro_rules! seprint_y {
  ($fmt:literal) => {
    eprintln!( concat!("⚡️ {}{}: ", $fmt, "{}"), YELLOW, LOG_VALUE, NC );
  };

  ($fmt:literal $(, $args:expr)*) => {
    eprintln!( concat!("⚡️ {}{}: ", $fmt, "{}"), YELLOW, LOG_VALUE, $($args),*, NC );
  };
}

#[macro_export]
macro_rules! seprint_g {
  ($fmt:literal) => {
    eprintln!( concat!("✅ {}{}: ", $fmt, "{}"), GREEN, LOG_VALUE, NC );
  };

  ($fmt:literal $(, $args:expr)*) => {
    eprintln!( concat!("✅ {}{}: ", $fmt, "{}"), GREEN, LOG_VALUE, $($args),*, NC );
  };
}

#[macro_export]
macro_rules! slog_r {
  ($log_level:ident, $fmt:literal) => {
    log::$log_level!( concat!("❌ {}{}: ", $fmt, "{}"), RED, LOG_VALUE, NC );
  };

  ($log_level:ident, $fmt:literal $(, $args:expr)*) => {
    log::$log_level!( concat!("❌ {}{}: ", $fmt, "{}"), RED, LOG_VALUE, $($args),*, NC );
  };
}

