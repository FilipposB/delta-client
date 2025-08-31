use std::collections::HashMap;
use std::{fs, thread};
use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::iter::Product;
use std::net::{TcpStream, UdpSocket};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{sleep, Thread};
use std::time::{Duration, Instant};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use uuid::Uuid;
use delta_lib::controller;
use delta_lib::controller::Controller;
use delta_lib::object::manifest::Manifest;
use delta_lib::object::progress_report::ProgressReport;
use delta_lib::request::{prepare_package, RequestData, RequestType};
use log::{debug, error, info};

fn main() {

    let addr = "127.0.0.1:3332";
    env_logger::init();


    let stream = TcpStream::connect("127.0.0.1:3332").unwrap();
    let addr = addr.parse().unwrap();
    let mut controller = Controller::new(stream, addr);

    let uuid = Uuid::from_str("d7bf0ada-7e54-4775-9309-ac2d41f6ee80").unwrap();
    let chunk_index: u64 = 5;
    let missed_chunks: Vec<u64> = vec![0, 3];

    let progress_report: ProgressReport = ProgressReport::new(uuid, chunk_index, missed_chunks);

    controller.write(RequestType::DownloadResource, Box::new(progress_report.clone()));
    let result = controller.read();

    match result {
        None => {}
        Some(result) => {
            match result {
                RequestData::DownloadResource(_) => {}
                RequestData::SendManifest(manifest) => {
                    download("download", &manifest, &progress_report);
                }
            }
        }
    }
}

fn ensure_folder(path: &str) -> std::io::Result<()> {
    let dir = Path::new(path);

    if !dir.exists() {
        create_dir_all(dir)?; // creates all parent directories too
    }

    Ok(())
}

fn download(location: &str, manifest: &Manifest, progress_report: &ProgressReport) {

    let udp_stream = UdpSocket::bind("0.0.0.0:0").unwrap();
    udp_stream.connect("127.0.0.1:3332").unwrap();

    udp_stream.set_nonblocking(false).unwrap();

    udp_stream.send(&manifest.id().as_bytes()[..]).expect("TODO: panic message");

    let start = Instant::now();

    let mut buffer = vec![0; manifest.chunk_size() as usize];
    debug!("Downloading {}", manifest.id());

    let path = Arc::new(format!("{}/{}", location, progress_report.id()));

    ensure_folder(&path).unwrap();

    let mut chunks_read =Arc::new(AtomicU64::new(0));

    let cache: Arc<RwLock<HashMap<u64, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
    let completed: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));


    let total = manifest.total_chunks();

    let m = MultiProgress::new();

    // Create separate progress bars
    let download_pb = m.add(ProgressBar::new(total));
    let save_files_pb = m.add(ProgressBar::new(total));

    // Set style for all bars
    let style = ProgressStyle::default_bar()
        .template(
            "{prefix:<20} {spinner:.green} [{elapsed_precise}] \
         [{bar:40.cyan/blue}] {pos:>5}/{len:<5} ({eta}) {msg:.green}"
        )
        .unwrap()
        .progress_chars("#>-");

    download_pb.set_style(style.clone());
    save_files_pb.set_style(style.clone());


    download_pb.set_prefix("Downloading Chunks");

    save_files_pb.set_prefix("Saving Chunks");


    let cache_thread = Arc::clone(&cache);
    let completed_thread = Arc::clone(&completed);
    let path_thread = Arc::clone(&path);


    let thread = thread::spawn(move || {
        let mut saved_files: u64 = 0;
        loop {
            if completed_thread.load(Ordering::Relaxed) && cache_thread.read().unwrap().is_empty() {
                continue;
            }

            let drained_cache: HashMap<u64, Vec<u8>> = {
                let mut write_lock = cache_thread.write().unwrap();
                std::mem::take(&mut *write_lock)
            };

            drained_cache.iter().for_each(|(chunk, payload)| {
                let file_path = format!("{}/{}", path_thread, chunk);
                fs::write(&file_path, payload).expect("Error");
                saved_files += 1;
            });
            save_files_pb.set_position(saved_files);

            if completed_thread.load(Ordering::Relaxed) && cache_thread.read().unwrap().is_empty()  {
                save_files_pb.finish_with_message("Save complete !");
                break;
            }
        }
    });


    let completed_thread_pb = Arc::clone(&completed);
    let chunks_read_thread_pb = Arc::clone(&chunks_read);


    let thread_download_progressbar = thread::spawn(move || {
        loop {

            download_pb.set_position(chunks_read_thread_pb.load(Ordering::Relaxed));

            if completed_thread_pb.load(Ordering::Relaxed) {
                download_pb.finish_with_message("Download complete !");
                break;
            }
        }
    });

    loop {
        let bytes_read = udp_stream.recv(&mut buffer);


        match bytes_read {
            Ok(bytes_read) => {

                if bytes_read == 0 {
                    continue;
                }

                let chunk =  u64::from_be_bytes(buffer[0..8].try_into().unwrap());
                

                let payload = &buffer[8..bytes_read];

                cache.write().unwrap().insert(chunk, payload.to_vec());

                if chunks_read.fetch_add(1, Ordering::Relaxed) + 1 >= manifest.total_chunks(){
                    break;
                }
            }
            Err(err) => {
                error!("Error: {}", err);
            }
        }
    }
    completed.store(true, Ordering::Relaxed);
    thread_download_progressbar.join().unwrap();
    thread.join().unwrap();

    let output_file_path = format!("{}/{}", location, manifest.name());

    let mut output_file = File::create(&output_file_path).expect("Error");


    let combine_files_pb = m.add(ProgressBar::new(total));
    combine_files_pb.set_style(style);
    combine_files_pb.set_prefix("Combining Chunks");
    let mut buffer = vec![0; manifest.chunk_size() as usize];

    for i in 0..manifest.total_chunks(){
        let file_path = format!("{}/{}", path, i);
        
        let mut bytes = manifest.payload_size() as usize;
        
        if i ==  manifest.total_chunks() - 1 {
            bytes = manifest.last_payload_bytes() as usize;
        }
        

        let mut file = File::open(&file_path).or_else(|e| {
           error!("Error: {}", file_path);
           Err(e)
        }).unwrap();
        file.read(&mut buffer).expect("Error");
        file.flush().expect("Error");
        output_file.write(&buffer[0..bytes]).expect("Error");
        output_file.flush().expect("Error");
        fs::remove_file(&file_path).expect("Error");
        combine_files_pb.set_position(i+1);
    }
    combine_files_pb.finish_with_message(format!("{output_file_path} !"));

    fs::remove_dir_all(path.as_str()).expect("Error");
    output_file.flush().unwrap()
}
