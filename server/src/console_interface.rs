use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::io::{self, Write};

use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::chat_server_api::ChatServerApi;

const DEFAULT_HOST: &str = "0.0.0.0";
const FALLBACK_PORT: u16 = 8080;
const INCORRECT_COMMAND: &str = "__INCORRECT_COMMAND__";

pub struct ServerConsoleInterface {
    api: Option<ChatServerApi>,
}

impl ServerConsoleInterface {
    pub fn new() -> Self {
        Self { api: None }
    }

    pub async fn start(mut self) {
        self.create_api();

        println!("RocketChat3000 server");
        println!("Введіть help для переліку команд.");

        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);

        loop {
            print!("RocketChatServer> ");
            io::stdout().flush().unwrap();

            let mut input = String::new();

            match reader.read_line(&mut input).await {
                Ok(0) => break,
                Ok(_) => println!("\nВведено: {}", input.trim()),
                Err(error) => {
                    eprintln!("Помилка читання: {error}");
                    break;
                }
            }

            if input.trim().is_empty() {
                continue;
            }

            let args = match parse_args(&input) {
                Ok(args) => args,
                Err(error) => {
                    println!("Команда некоректна: {error}");
                    continue;
                }
            };
            if args.is_empty() {
                continue;
            }

            match self.execute_command(&args).await {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) if error.to_string() == INCORRECT_COMMAND => {
                    println!("Команда некоректна");
                }
                Err(error) => {
                    println!("\n\nВиключення: {error:#}\n");
                }
            }
        }

        self.close().await;
    }

    fn create_api(&mut self) {
        if self.api.is_none() {
            self.api = Some(ChatServerApi::new());
        }
    }

    fn safe_get_api(&self) -> Result<&ChatServerApi> {
        self.api
            .as_ref()
            .ok_or_else(|| anyhow!("API is not initialized yet"))
    }

    async fn start_server(&mut self, port: u16) -> Result<()> {
        self.create_api();

        let addr: SocketAddr = self
            .safe_get_api()?
            .start(port)
            .await
            .with_context(|| {
                format!("Не вдалося запустити сервер на {DEFAULT_HOST}:{port}")
            })?;

        let display_ip = discover_local_ip()
            .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));

        println!(
            "Сервер запущено\nHost: {display_ip}\nPort: {}\nBind: {addr}",
            addr.port()
        );

        Ok(())
    }

    async fn stop_server(&mut self) -> Result<()> {
        let stopped = self.safe_get_api()?.stop_and_clear().await;
        println!("Сервер зупинено");
        Ok(())
    }

    async fn restart_server(&mut self, port: u16) -> Result<()> {
        self.create_api();
        if let Some(api) = self.api.as_ref() {
            api.stop_and_clear().await;
        }
        self.start_server(port).await?;
        println!("Сервер перезапущено");
        Ok(())
    }

    async fn execute_command(&mut self, args: &[String]) -> Result<bool> {
        match args[0].to_ascii_lowercase().as_str() {
            "start" => {
                let port = parse_port_command(args)?;
                self.start_server(port).await?;
            }
            "stop" => {
                check_count(args, 1)?;
                self.stop_server().await?;
            }
            "restart" => {
                let port = parse_port_command(args)?;
                self.restart_server(port).await?;
            }
            "status" => {
                check_count(args, 1)?;
                let api = self.safe_get_api()?;
                println!("API ініціалізовано");
                match api.is_running().await {
                    true => println!(
                        "Сервер працює"
                    ),
                    false => println!("Сервер не запущено"),
                }
            }
            "help" => {
                check_count(args, 1)?;
                print_help();
            }
            "exit" | "quit" => {
                check_count(args, 1)?;
                return Ok(true);
            }
            _ => return incorrect_result(),
        }
        Ok(false)
    }

    async fn close(&mut self) {
        if let Some(api) = self.api.take() {
            api.stop_and_clear().await;
        }
    }
}

impl Default for ServerConsoleInterface {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_port_command(args: &[String]) -> Result<u16> {
    let port = match args.len() {
        1 => FALLBACK_PORT,
        3 if args[1] == "-port" => args[2]
            .parse::<u16>()
            .map_err(|_| anyhow!("{INCORRECT_COMMAND}"))?,
        _ => return incorrect_result(),
    };

    if port == 0 {
        return Err(anyhow!("Некоректний порт: {port}. Допустимий діапазон: 1..=65535"));
    }
    Ok(port)
}

fn check_count(args: &[String], count: usize) -> Result<()> {
    if args.len() != count {
        return incorrect_result();
    }
    Ok(())
}

fn incorrect_result<T>() -> Result<T> {
    Err(anyhow!("{INCORRECT_COMMAND}"))
}

fn parse_args(input: &str) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut token_started = false;
    let mut escaped = false;

    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            token_started = true;
            continue;
        }

        match ch {
            '\\' if in_quotes => escaped = true,
            '"' => {
                in_quotes = !in_quotes;
                token_started = true;
            }
            c if c.is_whitespace() && !in_quotes => {
                if token_started {
                    args.push(std::mem::take(&mut current));
                    token_started = false;
                }
            }
            _ => {
                current.push(ch);
                token_started = true;
            }
        }
    }

    if escaped {
        current.push('\\');
    }
    if in_quotes {
        return Err(anyhow!("незакриті лапки"));
    }
    if token_started {
        args.push(current);
    }
    Ok(args)
}

fn discover_local_ip() -> Option<IpAddr> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("192.0.2.1:80").ok()?;
    Some(socket.local_addr().ok()?.ip())
}

fn print_help() {
    println!(
        r#"Доступні команди:

----- керування сервером -----
start                   Запуск сервера на порту 8080
start -port "port"      Запуск сервера на вказаному порту
stop                    Зупинка сервера та відключення клієнтів
restart                 Перезапуск сервера на порту 8080
restart -port "port"    Перезапуск на вказаному порту
status                  Перевірка стану API та сервера

----- інше -----
help                    Показати довідку
exit                    Завершити роботу"#
    );
}
