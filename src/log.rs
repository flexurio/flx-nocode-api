// create function to log message and save it to file

use crate::{ISDEBUG};
use colored::{Color, Colorize};

pub fn log_output(tipe: &str, title: &str, ssubtitle: &str, body: String, print_datetime: bool) {
    let mut subtitle = ssubtitle.to_string();
    if *ISDEBUG {
        // make subtitle length to 6
        if !tipe.contains("QUERY") && subtitle.len() < 6 {
            let diff = 6 - subtitle.len();
            for _ in 0..diff {
                subtitle.push(' ');
            }
        }
        let mut s_datetime = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        if !print_datetime {
            s_datetime = "".to_string();
        }

        let color_tipe = match tipe {
            "INFO" => Color::Green,
            "ERROR" => Color::Red,
            "WARN" => Color::Yellow,
            "DEBUG" => Color::Blue,
            "QUERY" => Color::Cyan,
            _ => Color::White,
        };

        let s_message_1 = format!(
            "# {}, {} {} at {}: ",
            tipe.color(color_tipe),
            title.cyan(),
            subtitle.color(color_tipe),
            s_datetime
        );
        print!("{}", s_message_1);
        if tipe.contains("QUERY") {
            println!("\n");
        }
        println!("{}", body.color(color_tipe));
        if tipe.contains("QUERY") {
            println!("\n");
        }
    }

}