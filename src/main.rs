use std::io;

mod server;

fn main() -> io::Result<()> {
    server::start_server("127.0.0.1:9092")
}
