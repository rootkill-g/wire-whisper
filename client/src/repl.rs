//! Interactive REPL.
//!
//! A single `select!` loop multiplexes stdin and the server socket. When a
//! frame arrives mid-prompt, the prompt is erased with `\r\x1b[K` (CR +
//! clear-to-end-of-line), the frame is printed, and the prompt is re-drawn.
//! Cheap UX trick, no raw mode required.

use std::io::Write;

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use whisper_protocol::{ClientFrame, ClientSideCodec, ServerFrame};

/// Drive the REPL until the user leaves or the server disconnects.
pub async fn run(mut socket: Framed<TcpStream, ClientSideCodec>, username: &str) -> Result<()> {
    let prompt = format!("{username}> ");
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    print_help();
    print_prompt(&prompt);

    loop {
        tokio::select! {
            biased;

            line = lines.next_line() => {
                let Some(line) = line.context("reading stdin")? else {
                    eprintln!("\r[client] stdin closed; sending leave");
                    let _ = socket.send(ClientFrame::Leave).await;
                    break;
                };
                match parse_command(&line) {
                    Command::Send(body) => {
                        socket.send(ClientFrame::Send { body }).await
                            .context("sending message")?;
                    }
                    Command::Leave => {
                        socket.send(ClientFrame::Leave).await
                            .context("sending leave")?;
                        break;
                    }
                    Command::Blank => {}
                    Command::Invalid(help) => {
                        println!("[client] {help}");
                    }
                    Command::Unknown(s) => {
                        println!("[client] unknown command: '{s}'");
                        println!("[client] available: send <msg>, leave");
                    }
                }
                print_prompt(&prompt);
            }

            frame = socket.next() => {
                let Some(frame) = frame else {
                    eprintln!("\r[client] server closed the connection");
                    break;
                };
                let frame = frame.context("decoding server frame")?;
                match frame {
                    ServerFrame::Ping => {
                        // Auto-respond — the heartbeat is invisible UX.
                        // We *don't* erase or re-print the prompt; nothing
                        // visible changed.
                        socket.send(ClientFrame::Pong).await
                            .context("responding to Ping")?;
                    }
                    other => {
                        erase_prompt();
                        match other {
                            ServerFrame::Message { from, body } => {
                                println!("<{from}> {body}");
                            }
                            ServerFrame::Joined { username: u } => {
                                println!("[room] {u} joined");
                            }
                            ServerFrame::Departed { username: u } => {
                                println!("[room] {u} left");
                            }
                            ServerFrame::Error { reason } => {
                                println!("[server-error] {reason}");
                            }
                            ServerFrame::Welcome { motd, occupancy } => {
                                println!("[server] {motd} (room occupancy: {occupancy})");
                            }
                            ServerFrame::Rejected { reason } => {
                                println!("[server] rejected: {reason}");
                                print_prompt(&prompt);
                                break;
                            }
                            ServerFrame::Ping => unreachable!("filtered above"),
                        }
                        print_prompt(&prompt);
                    }
                }
            }
        }
    }
    Ok(())
}

/// The user's parsed intent.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    Send(String),
    Leave,
    Blank,
    Invalid(String),
    Unknown(String),
}

fn parse_command(line: &str) -> Command {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Command::Blank;
    }
    // `split_once` returns `None` only when the separator isn't found —
    // then the whole input is the head and the tail is empty. No `unwrap`.
    let (head, tail) = trimmed
        .split_once(char::is_whitespace)
        .unwrap_or((trimmed, ""));
    let tail = tail.trim();
    match head {
        "send" => {
            if tail.is_empty() {
                Command::Invalid("usage: send <msg>".into())
            } else {
                Command::Send(tail.to_string())
            }
        }
        "leave" => {
            if !tail.is_empty() {
                Command::Invalid("usage: leave (takes no arguments)".into())
            } else {
                Command::Leave
            }
        }
        _ => Command::Unknown(head.to_string()),
    }
}

fn print_help() {
    println!("[client] commands: send <msg>, leave");
}

fn print_prompt(prompt: &str) {
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(prompt.as_bytes());
    let _ = out.flush();
}

fn erase_prompt() {
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(b"\r\x1b[K");
    let _ = out.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_send() {
        assert_eq!(
            parse_command("send hello world"),
            Command::Send("hello world".into())
        );
    }

    #[test]
    fn parses_send_trims_trailing_whitespace() {
        assert_eq!(
            parse_command("send   spaced out   "),
            Command::Send("spaced out".into())
        );
    }

    #[test]
    fn parses_leave() {
        assert_eq!(parse_command("leave"), Command::Leave);
        assert_eq!(parse_command("  leave  "), Command::Leave);
    }

    #[test]
    fn leave_with_args_is_invalid() {
        assert!(matches!(parse_command("leave now"), Command::Invalid(_)));
    }

    #[test]
    fn blank_lines() {
        assert_eq!(parse_command(""), Command::Blank);
        assert_eq!(parse_command("   \t  "), Command::Blank);
    }

    #[test]
    fn send_without_body_is_invalid() {
        assert!(matches!(parse_command("send"), Command::Invalid(_)));
        assert!(matches!(parse_command("send   "), Command::Invalid(_)));
    }

    #[test]
    fn unknown_command() {
        assert_eq!(parse_command("quit"), Command::Unknown("quit".into()));
    }
}
