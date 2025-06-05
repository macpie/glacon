use apache_avro::Reader;
use std::{
    env,
    fs::{self, File},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = env::args().collect::<Vec<_>>();
    let directory = args[1].clone();

    tracing::info!("Getting files from {}", directory);

    let files = find_avro_files(&directory)?;
    for file_name in files {
        let read_file = File::open(file_name.clone())?;
        let reader = Reader::new(read_file)?;
        let write_file = File::create(format!("{}.json", file_name))?;

        let mut data = vec![];
        for v in reader {
            let v = v?;
            let json = serde_json::Value::try_from(v)?;
            data.push(json);
        }

        serde_json::to_writer_pretty(write_file, &data)?;

        tracing::info!("Read file {}", file_name);
    }

    Ok(())
}

pub fn find_avro_files(dir: &str) -> std::io::Result<Vec<String>> {
    let mut avro_files = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() && path.extension().map_or(false, |ext| ext == "avro") {
            if let Some(path_str) = path.to_str() {
                avro_files.push(path_str.to_string());
            }
        }
    }

    Ok(avro_files)
}
