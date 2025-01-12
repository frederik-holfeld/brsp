use clap::{Parser, Subcommand};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Upload,
    Render,
    Delete,
    Serve,
}

fn main() {
    let args = Cli::parse();

    match args.command {
        Command::Upload => {
            todo!();
        }
        Command::Render => {
            todo!();
        }
        Command::Delete => {
            todo!();
        }
        Command::Serve => {
            todo!();
        }
    }
}
