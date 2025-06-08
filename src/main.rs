use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::{
    env::set_current_dir,
    fs::{create_dir, read, remove_file, write},
    hash::{DefaultHasher, Hash, Hasher},
    io::{ErrorKind, Read, Write},
    net::{Ipv6Addr, TcpListener, TcpStream},
    path::PathBuf,
    process,
    sync::{Condvar, Mutex},
    thread,
};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Upload {
        ips: String,
        id: String,
        blend: PathBuf,
    },
    Render {
        ips: String,
        output_dir: PathBuf,
        id: String,
        frames: String,
    },
    Delete,
    Serve {
        brpy: PathBuf,
        work_dir: PathBuf,

        #[arg(short, long)]
        blender: Option<PathBuf>,
    },
    Query {
        ips: String,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Request {
    Upload { id: String, size: usize },
    Render,
    Delete,
    Query,
}

#[derive(Serialize, Deserialize)]
struct FrameRequest {
    id: String,
    frame: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Response {
    Okay,
    Fail { message: String },
}

#[derive(Serialize, Deserialize)]
enum RenderAcceptResponse {
    Accept,
    Reject,
}

#[derive(Serialize, Deserialize)]
enum RenderResponse {
    Okay { size: usize, extension: String },
    Fail,
}

#[derive(Serialize, Deserialize)]
struct QueryResponse {
    version: [u8; 3],
    compute_device_type: String,
    devices: ComputeDeviceList,
}

#[derive(Serialize, Deserialize, Clone)]
struct ComputeDeviceList {
    active: Vec<String>,
    inactive: Vec<String>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum BrpyRequest {
    Render {
        blend: PathBuf,
        frame: usize,
        output: PathBuf,
    },
    Query,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum BrpyRenderResponse {
    Okay { image: PathBuf },
    Fail,
}

fn main() {
    let args = Cli::parse();

    match args.command {
        Command::Upload { ips, id, blend } => {
            let ips = ips.split_terminator(',');

            let mut blend = read(blend).unwrap();
            let mut request = to_header(
                serde_json::to_vec(&Request::Upload {
                    id,
                    size: blend.len(),
                })
                .unwrap(),
            );
            request.append(&mut blend);

            thread::scope(|scope| {
                for ip in ips {
                    scope.spawn(|| {
                        upload(ip, &request);
                    });
                }
            });
        }
        Command::Render {
            ips,
            output_dir,
            id,
            frames,
        } => {
            set_current_dir(output_dir).unwrap();

            let frames = {
                let mut list = Vec::new();

                for range in frames.split_terminator(',') {
                    let frame = range.parse::<usize>();
                    match frame {
                        Ok(frame) => {
                            list.push(frame);
                        }
                        Err(_) => {
                            let range: Vec<&str> = range.split_terminator("..").collect();
                            let start: usize = range[0].parse().unwrap();
                            let end: usize = range[1].parse().unwrap();

                            list.append(&mut (start..=end).collect());
                        }
                    }
                }

                list.sort();
                list.dedup();
                list.reverse();

                Mutex::new(list)
            };

            thread::scope(|scope| {
                for ip in ips.split_terminator(',') {
                    scope.spawn(|| {
                        render(ip, &id, &frames);
                    });
                }
            })
        }
        Command::Delete => {
            todo!();
        }
        Command::Query { ips } => {
            let header = to_header(serde_json::to_vec(&Request::Query).unwrap());

            thread::scope(|scope| {
                for ip in ips.split_terminator(',') {
                    scope.spawn(|| {
                        query(ip, &header);
                    });
                }
            });
        }
        Command::Serve {
            brpy,
            work_dir,
            blender,
        } => {
            if !brpy.is_file() {
                panic!(
                    "BRPy script {} either does not exist, access is not permitted or it's not a file",
                    brpy.display()
                );
            }

            let blender = match blender {
                None => PathBuf::from("blender"),
                Some(blender) => blender.canonicalize().unwrap(),
            };

            set_current_dir(work_dir).unwrap();

            if let Err(error) = create_dir("anonymous") {
                match error.kind() {
                    ErrorKind::AlreadyExists => {}
                    _ => {
                        panic!("{}", error);
                    }
                }
            }

            let listener = match TcpListener::bind((Ipv6Addr::UNSPECIFIED, 21816)) {
                Ok(listener) => listener,
                Err(_) => TcpListener::bind((Ipv6Addr::UNSPECIFIED, 0)).unwrap(),
            };

            let mut brpy = {
                let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).unwrap();
                let port = listener.local_addr().unwrap().port();

                process::Command::new(blender)
                    .args([
                        "--background",
                        "--python",
                        brpy.to_str().unwrap(),
                        "--",
                        &port.to_string(),
                    ])
                    .spawn()
                    .unwrap();

                listener.accept().unwrap().0
            };

            let info: QueryResponse = {
                let request = to_header(serde_json::to_vec(&BrpyRequest::Query).unwrap());
                brpy.write_all(&request).unwrap();

                serde_json::from_slice(&read_header(&mut brpy).unwrap()).unwrap()
            };

            let render_requesters: Mutex<Vec<Option<TcpStream>>> = Mutex::new(vec![None]);
            let notifier = Condvar::new();

            thread::scope(|scope| {
                scope.spawn(|| {
                    worker_brpy(brpy, &render_requesters, &notifier);
                });

                println!(
                    "Listening on port {}",
                    listener.local_addr().unwrap().port()
                );

                for stream in listener.incoming() {
                    match stream {
                        Ok(stream) => {
                            scope.spawn(|| {
                                handle_client(stream, &info, &render_requesters, &notifier);
                            });
                        }
                        Err(error) => {
                            println!("Failed to establish new connection: {}", error);
                        }
                    }
                }
            })
        }
    }
}

fn handle_client(
    mut client: TcpStream,
    info: &QueryResponse,
    render_requesters: &Mutex<Vec<Option<TcpStream>>>,
    notifier: &Condvar,
) {
    loop {
        let request = serde_json::from_slice(&read_header(&mut client).unwrap()).unwrap();

        match request {
            Request::Upload { id, size } => {
                let mut blend = vec![0; size];
                client.read_exact(&mut blend).unwrap();

                let mut hasher = DefaultHasher::new();
                id.hash(&mut hasher);
                let hash = hasher.finish();

                let _ = create_dir(format!("anonymous/{}", hash));
                let header = match write(format!("anonymous/{0}/{0}.blend", hash), blend) {
                    Ok(()) => serde_json::to_vec(&Response::Okay).unwrap(),
                    Err(_) => serde_json::to_vec(&Response::Fail {
                        message: "Could not save file".to_string(),
                    })
                    .unwrap(),
                };

                let response = to_header(header);
                client.write_all(&response).unwrap();

                println!("Saved .blend file with ID \"{}\"", id);
                break;
            }
            Request::Render => {
                let requester = Some(client);

                let mut free_slot = 0;
                let mut free_slot_found = false;

                {
                    let mut render_requesters = render_requesters.lock().unwrap();
                    let len = render_requesters.len();

                    for slot in 0..len {
                        if render_requesters[slot].is_none() {
                            free_slot_found = true;
                            free_slot = slot;
                            break;
                        }
                    }

                    if free_slot_found {
                        render_requesters[free_slot] = requester;
                        println!("Put new render requester in slot {}", free_slot);
                    } else {
                        render_requesters.push(requester);
                        println!("Created render slot {} for new render requester", len);
                    }
                }

                notifier.notify_all();

                return;
            }
            Request::Delete => {
                todo!();
            }
            Request::Query => {
                let response = to_header(
                    serde_json::to_vec(&QueryResponse {
                        version: info.version,
                        compute_device_type: info.compute_device_type.clone(),
                        devices: info.devices.clone(),
                    })
                    .unwrap(),
                );

                client.write_all(&response).unwrap();
            }
        }
    }
}

fn render(ip: &str, id: &str, frames: &Mutex<Vec<usize>>) {
    let mut server = connect(ip);

    let request = to_header(serde_json::to_vec(&Request::Render).unwrap());
    server.write_all(&request).unwrap();

    loop {
        if frames.lock().unwrap().is_empty() {
            return;
        }

        let response = read_header(&mut server).unwrap();
        let response = serde_json::from_slice(&response).unwrap();

        match response {
            RenderAcceptResponse::Accept => {
                println!("Render request accepted");

                let frame = match frames.lock().unwrap().pop() {
                    None => {
                        return;
                    }
                    Some(frame) => frame,
                };

                let request = to_header(
                    serde_json::to_vec(&FrameRequest {
                        id: String::from(id),
                        frame,
                    })
                    .unwrap(),
                );
                server.write_all(&request).unwrap();

                let header = read_header(&mut server).unwrap();
                let header = serde_json::from_slice(&header).unwrap();

                match header {
                    RenderResponse::Okay { size, extension } => {
                        let mut image = vec![0; size];
                        server.read_exact(&mut image).unwrap();

                        let image_name = format!("{:04}.{}", frame, extension);
                        write(&image_name, image).unwrap();
                        println!("Saved frame {} as {}", frame, image_name);
                    }
                    RenderResponse::Fail => {
                        todo!();
                    }
                }
            }
            RenderAcceptResponse::Reject => {
                todo!();
            }
        }
    }
}

fn upload(ip: &str, request: &[u8]) {
    let mut server = connect(ip);
    server.write_all(request).unwrap();

    let header = read_header(&mut server).unwrap();
    let header: Response = serde_json::from_slice(&header).unwrap();

    match header {
        Response::Okay => {
            println!("File uploaded successfully");
        }
        Response::Fail { message } => {
            println!("File upload failed\nReason: {}", message);
        }
    }
}

fn read_header(stream: &mut TcpStream) -> Result<Vec<u8>, std::io::Error> {
    let mut len = [0; 2];
    stream.read_exact(&mut len)?;

    let mut header = vec![0; u16::from_le_bytes(len) as usize];
    stream.read_exact(&mut header)?;

    Ok(header)
}

fn to_header(mut content: Vec<u8>) -> Vec<u8> {
    let mut header = u16::try_from(content.len()).unwrap().to_le_bytes().to_vec();
    header.append(&mut content);

    header
}

fn connect(ip: &str) -> TcpStream {
    match TcpStream::connect(ip) {
        Ok(stream) => stream,
        Err(error) => match error.kind() {
            ErrorKind::InvalidInput => TcpStream::connect((ip, 21816)).unwrap(),
            _ => {
                panic!("{:?}", error);
            }
        },
    }
}

fn query(ip: &str, request: &[u8]) {
    let mut server = connect(ip);
    server.write_all(request).unwrap();

    let header = read_header(&mut server).unwrap();
    let header: QueryResponse = serde_json::from_slice(&header).unwrap();

    let mut output = format!(
        "{}:\n    Blender version: {}.{}.{}\n    Compute device type: {}",
        ip, header.version[0], header.version[1], header.version[2], header.compute_device_type
    );

    let active_not_empty = !header.devices.active.is_empty();
    let inactive_not_empty = !header.devices.inactive.is_empty();

    if active_not_empty || inactive_not_empty {
        output += "\n    Devices:";

        if active_not_empty {
            output += "\n        Active:";
            for device in header.devices.active {
                output += &format!("\n            {}", device);
            }
        }

        if inactive_not_empty {
            output += "\n        Inactive:";
            for device in header.devices.inactive {
                output += &format!("\n            {}", device);
            }
        }
    }

    println!("{}", output);
}

fn worker_brpy(
    mut brpy: TcpStream,
    requesters: &Mutex<Vec<Option<TcpStream>>>,
    notifier: &Condvar,
) {
    let mut slot = 0;

    'outer: loop {
        let frame_request: FrameRequest = {
            let old_slot = slot;
            let mut requesters = requesters.lock().unwrap();

            let frame_request = {
                let len = requesters.len();
                let client = loop {
                    slot = (slot + 1) % len;
                    match &mut requesters[slot] {
                        None => {
                            if slot == old_slot {
                                println!("Awaiting further render requests");
                                let _requesters = notifier.wait(requesters).unwrap();
                                continue 'outer;
                            }
                        }
                        Some(requester) => {
                            break requester;
                        }
                    }
                };

                let request = to_header(serde_json::to_vec(&RenderAcceptResponse::Accept).unwrap());
                let _ = client.write_all(&request);

                read_header(client)
            };

            match frame_request {
                Err(_) => {
                    requesters[slot] = None;
                    continue;
                }
                Ok(frame_request) => serde_json::from_slice(&frame_request).unwrap(),
            }
        };

        println!("Rendering slot {}", slot);

        let mut hasher = DefaultHasher::new();
        frame_request.id.hash(&mut hasher);
        let hash = hasher.finish();

        if let Err(error) = create_dir(format!("anonymous/{}/render", hash)) {
            match error.kind() {
                ErrorKind::AlreadyExists => {}
                _ => {
                    panic!("{}", error);
                }
            }
        }

        let request = to_header(
            serde_json::to_vec(&BrpyRequest::Render {
                blend: format!("anonymous/{0}/{0}.blend", hash).into(),
                frame: frame_request.frame,
                output: format!("anonymous/{}/render", hash).into(),
            })
            .unwrap(),
        );

        brpy.write_all(&request).unwrap();
        let response = serde_json::from_slice(&read_header(&mut brpy).unwrap()).unwrap();

        match response {
            BrpyRenderResponse::Okay { image } => {
                let extension = String::from(image.extension().unwrap().to_str().unwrap());
                let mut image_data = read(&image).unwrap();

                let mut response = to_header(
                    serde_json::to_vec(&RenderResponse::Okay {
                        size: image_data.len(),
                        extension,
                    })
                    .unwrap(),
                );
                response.append(&mut image_data);

                {
                    let mut requesters = requesters.lock().unwrap();
                    let client = &mut requesters[slot].as_ref().unwrap();

                    if { client.write_all(&response) }.is_err() {
                        println!("Cannot reach client, discarding frame");
                        requesters[slot] = None;
                    } else {
                        println!(
                            "Rendered frame {} of \"{}\" sent to client",
                            frame_request.frame, frame_request.id
                        );
                    }
                }

                let _ = remove_file(image);
            }
            BrpyRenderResponse::Fail => {
                todo!();
            }
        }
    }
}
