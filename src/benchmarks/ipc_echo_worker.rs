use std::io::{self, BufWriter, Read, Write};

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut input = stdin.lock();
    let mut output = BufWriter::new(stdout.lock());
    let mut length_bytes = [0_u8; 4];

    loop {
        match input.read_exact(&mut length_bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        }
        let payload_length = u32::from_le_bytes(length_bytes) as usize;
        let mut payload = vec![0_u8; payload_length];
        input.read_exact(&mut payload)?;
        output.write_all(&length_bytes)?;
        output.write_all(&payload)?;
        output.flush()?;
    }
}
