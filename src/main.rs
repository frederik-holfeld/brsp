use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::{
    env::set_current_dir,
    fs::{create_dir, read, write},
    hash::{DefaultHasher, Hash, Hasher},
    io::{ErrorKind::AlreadyExists, Read, Write},
    net::{Ipv6Addr, SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process, thread,
};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Upload {
        ip: SocketAddr,
        id: String,
        blend: PathBuf,
    },
    Render {
        ip: SocketAddr,
        output_dir: PathBuf,
        id: String,
        frames: String,
    },
    Delete,
    Serve {
        brpy: PathBuf,
        work_dir: PathBuf,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Request {
    Upload { id: String, size: usize },
    Render { id: String, frame: usize },
    Delete,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Response {
    Okay,
    Fail { message: String },
}

#[derive(Serialize, Deserialize)]
enum RenderResponse {
    Okay { size: usize, extension: String },
    Fail,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum BrpyRequest {
    Render {
        blend: PathBuf,
        frame: usize,
        output: PathBuf,
    },
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
        Command::Upload { ip, id, blend } => {
            let mut blend = read(blend).unwrap();
            let mut header = serde_json::to_vec(&Request::Upload {
                id,
                size: blend.len(),
            })
            .unwrap();

            let mut request = vec![header.len().try_into().unwrap()];
            request.append(&mut header);
            request.append(&mut blend);

            let mut server = TcpStream::connect(ip).unwrap();
            server.write_all(&request).unwrap();

            let mut len = [0; 1];
            server.read_exact(&mut len).unwrap();

            let mut header = vec![0; len[0] as usize];
            server.read_exact(&mut header).unwrap();

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
        Command::Render {
            ip,
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
                list
            };

            let mut server = TcpStream::connect(ip).unwrap();

            for frame in frames {
                let mut header = serde_json::to_vec(&Request::Render {
                    id: id.clone(),
                    frame,
                })
                .unwrap();

                let mut request = vec![header.len().try_into().unwrap()];
                request.append(&mut header);

                server.write_all(&request).unwrap();

                let mut len = [0; 1];
                server.read_exact(&mut len).unwrap();

                let mut header = vec![0; len[0] as usize];
                server.read_exact(&mut header).unwrap();

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
        }
        Command::Delete => {
            todo!();
        }
        Command::Serve { brpy, work_dir } => {
            if !brpy.is_file() {
                panic!("BRPy script {} either does not exist, access is not permitted or it's not a file", brpy.display());
            }

            set_current_dir(work_dir).unwrap();

            if let Err(error) = create_dir("anonymous") {
                match error.kind() {
                    AlreadyExists => {}
                    _ => {
                        panic!("{}", error);
                    }
                }
            }

            let listener = match TcpListener::bind((Ipv6Addr::UNSPECIFIED, 21816)) {
                Ok(listener) => listener,
                Err(_) => TcpListener::bind((Ipv6Addr::UNSPECIFIED, 0)).unwrap(),
            };

            println!(
                "Listening on port {}",
                listener.local_addr().unwrap().port()
            );

            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let brpy = brpy.clone();

                        thread::spawn(move || {
                            handle_client(stream, brpy);
                        });
                    }
                    Err(error) => {
                        println!("Failed to establish new connection: {}", error);
                    }
                }
            }
        }
    }
}

fn handle_client(mut client: TcpStream, brpy: PathBuf) {
    loop {
        let mut len = [0; 1];
        client.read_exact(&mut len).unwrap();

        let mut header = vec![0; len[0] as usize];
        client.read_exact(&mut header).unwrap();
        let header: Request = serde_json::from_slice(&header).unwrap();

        match header {
            Request::Upload { id, size } => {
                let mut blend = vec![0; size];
                client.read_exact(&mut blend).unwrap();

                let mut hasher = DefaultHasher::new();
                id.hash(&mut hasher);
                let hash = hasher.finish();

                let mut header;
                let _ = create_dir(format!("anonymous/{}", hash));
                match write(format!("anonymous/{0}/{0}.blend", hash), blend) {
                    Ok(()) => {
                        header = serde_json::to_vec(&Response::Okay).unwrap();
                    }
                    Err(_) => {
                        header = serde_json::to_vec(&Response::Fail {
                            message: "Could not save file".to_string(),
                        })
                        .unwrap();
                    }
                }

                let mut response = vec![header.len().try_into().unwrap()];
                response.append(&mut header);
                client.write_all(&response).unwrap();

                println!("Saved .blend file with ID \"{}\"", id);
                break;
            }
            Request::Render { id, frame } => {
                let mut hasher = DefaultHasher::new();
                id.hash(&mut hasher);
                let hash = hasher.finish();

                if let Err(error) = create_dir(format!("anonymous/{}/render", hash)) {
                    match error.kind() {
                        AlreadyExists => {}
                        _ => {
                            panic!("{}", error);
                        }
                    }
                }

                let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).unwrap();
                let port = listener.local_addr().unwrap().port();

                let mut process = process::Command::new("blender")
                    .args([
                        "--background",
                        "--python",
                        brpy.to_str().unwrap(),
                        "--",
                        &port.to_string(),
                    ])
                    .spawn()
                    .unwrap();

                let (mut brpy, _) = listener.accept().unwrap();

                let mut header = serde_json::to_vec(&BrpyRequest::Render {
                    blend: format!("anonymous/{0}/{0}.blend", hash).into(),
                    frame,
                    output: format!("anonymous/{}/render", hash).into(),
                })
                .unwrap();

                let mut request = vec![header.len().try_into().unwrap()];
                request.append(&mut header);

                brpy.write_all(&request).unwrap();

                let mut len = [0; 1];
                brpy.read_exact(&mut len).unwrap();

                let mut header = vec![0; len[0] as usize];
                brpy.read_exact(&mut header).unwrap();

                let header = serde_json::from_slice(&header).unwrap();
                match header {
                    BrpyRenderResponse::Okay { image } => {
                        let extension = String::from(image.extension().unwrap().to_str().unwrap());
                        let mut image = read(image).unwrap();

                        let mut header = serde_json::to_vec(&RenderResponse::Okay {
                            size: image.len(),
                            extension,
                        })
                        .unwrap();

                        let mut response = vec![header.len().try_into().unwrap()];
                        response.append(&mut header);
                        response.append(&mut image);

                        client.write_all(&response).unwrap();
                    }
                    BrpyRenderResponse::Fail => {
                        todo!();
                    }
                }

                let _ = process.wait();

                println!("Rendered frame {} of \"{}\" sent to client", frame, id);
            }
            Request::Delete => {
                todo!();
            }
        }
    }
}
