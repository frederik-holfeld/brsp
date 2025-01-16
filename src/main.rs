use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::{
    fs::{create_dir, read, write},
    hash::{DefaultHasher, Hash, Hasher},
    io::{ErrorKind::AlreadyExists, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
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
        ip: SocketAddr,
        id: String,
        blend: PathBuf,
    },
    Render,
    Delete,
    Serve,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Request {
    Upload { id: String, size: usize },
    Render,
    Delete,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Response {
    Okay,
    Fail { message: String },
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
        Command::Render => {
            todo!();
        }
        Command::Delete => {
            todo!();
        }
        Command::Serve => {
            if let Err(error) = create_dir("anonymous") {
                match error.kind() {
                    AlreadyExists => {}
                    _ => {
                        panic!("{}", error);
                    }
                }
            }

            let listener = match TcpListener::bind("0.0.0.0:21816") {
                Ok(listener) => listener,
                Err(_) => TcpListener::bind("0.0.0.0:0").unwrap(),
            };

            println!(
                "Listening on port {}",
                listener.local_addr().unwrap().port()
            );

            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        thread::spawn(move || {
                            handle_client(stream);
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

fn handle_client(mut client: TcpStream) {
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
            Request::Render => {
                todo!();
            }
            Request::Delete => {
                todo!();
            }
        }
    }
}
