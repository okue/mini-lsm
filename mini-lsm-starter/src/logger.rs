use env_logger::{Target, WriteStyle};
use log::LevelFilter;
use std::io::Write;

pub fn setup() {
    env_logger::builder()
        // intentionally set to false so that debugger doesn't capture.
        .is_test(false)
        .write_style(WriteStyle::Always)
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .format(|buf, record| {
            let style = buf.default_level_style(record.level());
            writeln!(
                buf,
                "[{time} {style}{level}{style:#} {file}:{line}] {msg}",
                time = buf.timestamp(),
                level = record.level(),
                style = style,
                file = record.file().unwrap_or("unknown"),
                line = record.line().unwrap_or(0),
                msg = record.args(),
            )
        })
        .parse_default_env()
        .init();
}
