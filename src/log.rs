// create function to log message and save it to file

use crate::{ISDEBUG, ISLOGGING, LOC_LOGGING};
use colored::Colorize;
use regex::Regex;
use std::fs::OpenOptions;
use std::io::Write;

pub fn log_to_file(message: &str) {
    let file_path = LOC_LOGGING.clone() + "/.log.txt";
    let mut file = match OpenOptions::new().create(true).append(true).open(&file_path) {
        Ok(file) => file,
        Err(e) => {
            // create directory if not exists
            if let Some(parent) = std::path::Path::new(&file_path).parent() {
                if !parent.exists() {
                    if let Err(e) = std::fs::create_dir_all(parent) {
                        eprintln!("Failed to create log directory: {}", e);
                        return;
                    }
                }
            }

            // create log file if not exists
            if let Some(parent) = std::path::Path::new(&file_path).parent() {
                if !parent.exists() {
                    if let Err(e) = std::fs::create_dir_all(parent) {
                        eprintln!("Failed to create log directory: {}", e);
                        return;
                    }
                }
            }
            eprintln!("Failed to open log file: {}", e);
            return;
        }
    };
    // Regex untuk mendeteksi karakter ANSI escape
    let ansi_escape = Regex::new(r"\x1B\[[0-9;]*[mK]").unwrap();
    let clean_message = ansi_escape.replace_all(message, "").to_string();    
    if let Err(e) = writeln!(file, "{}", clean_message) {
        eprintln!("Failed to write to log file: {}", e);
    }
}



pub fn log_output(tipe:&str, title:&str, ssubtitle:&str, body:String, print_datetime:bool) {
    let mut subtitle = ssubtitle.to_string();
    let mut s_message_1 = "".to_string();
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
        
        s_message_1 = format!("# {}, {} {} at {}: ",tipe.yellow(), title.cyan(), subtitle.blue(), s_datetime);
        print!("{}", s_message_1);
        if tipe.contains("QUERY") {
            println!("\n");
        }
        println!("{}", body.green());
        if tipe.contains("QUERY") {
            println!("\n");
        }
    }

    if *ISLOGGING {
        log_to_file(s_message_1.as_str());
        log_to_file(&body);
    }

}
