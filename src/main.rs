mod daskcodec;

use tokio::net::TcpListener;
use tokio::prelude::*;
use failure::Error;

use daskcodec::DaskCodec;
use tokio::codec::Framed;
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("STARTED");

    let mut listener = TcpListener::bind("127.0.0.1:7070").await?;
    loop {
        let (socket, _) = listener.accept().await?;
        println!("New connection");
        socket.set_nodelay(true).unwrap();
        let mut framed = Framed::new(socket, DaskCodec::new());

        let data = Bytes::from(&b"this should crash"[..]);
        framed.send(data).await.unwrap();

        println!("Send finished!");

        let (buffer, framed) = framed.into_future().await;
        println!("{:?}", buffer);
    }
/*    for stream in listener.incoming() {
        dbg!(stream?);
    }*/
}
