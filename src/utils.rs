use std::io::BufRead;

pub fn read_sqls(file_blob_pattern: &str) -> Vec<String> {
    let mut sqls = Vec::new();

    // Use the glob function to get an iterator over entries matching the pattern
    for entry in glob::glob(file_blob_pattern).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => {
                // If the path is a file, read lines from the file
                if path.is_file() {
                    let file = std::fs::File::open(path).unwrap();
                    let reader = std::io::BufReader::new(file);

                    let mut sql = String::new();
                    for line in reader.lines() {
                        let line = line.unwrap(); // In real code, handle errors properly
                        if line.trim().is_empty() {
                            continue;
                        }
                        if line.trim().ends_with(";") {
                            sql.push('\n');
                            sql.push_str(&line);
                            sqls.push(sql.trim().trim_end_matches(';').to_string());
                            sql = String::new();
                        } else {
                            sql.push_str(&line);
                        }
                    }
                    if sql.trim().len() > 0 {
                        sqls.push(sql.trim().to_string());
                    }
                }
            }
            Err(e) => panic!("Error processing entry: {}", e),
        }
    }

    sqls
}
