use nginx_car_range::{nginx_handler, MockRequest};
use std::fs::File;
use std::io::{self, BufReader};

fn main() -> anyhow::Result<()> {
    let car_file = File::open("iconfixture.car")?;
    let car_reader = BufReader::new(car_file);

    let mut buff = io::Cursor::new(vec![0; 2000]);

    let req = MockRequest::new(car_reader, &mut buff, 2000..4000);

    nginx_handler(req)?;

    Ok(())
}
