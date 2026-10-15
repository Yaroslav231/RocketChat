use std::io::Write;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::task::JoinHandle;

use crate::chat_api::{ChatApi, ChatEvent};

const INCORRECT_COMMAND: &str = "__INCORRECT_COMMAND__";

pub struct ConsoleInterface {
    api: Option<Arc<ChatApi>>,
    collect_join_handle: Option<JoinHandle<()>>,
    recipient_id: Option<i64>,
}

impl ConsoleInterface {
    pub fn new() -> Self {
        Self {
            api: None,
            collect_join_handle: None,
            recipient_id: None,
        }
    }

    pub async fn start(mut self) {
        self.initialize_api();
        self.start_collect_messages();

        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);

        loop {
            print!("\nRocketChat> ");
            let _ = std::io::stdout().flush();

            let mut input = String::new();
            match reader.read_line(&mut input).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) => {
                    println!("Не вдалося прочитати команду: {error}");
                    continue;
                }
            }

            if input.trim().is_empty() {
                continue;
            }

            let args = match parse_args(&input) {
                Ok(args) => args,
                Err(_) => {
                    incorrect_command();
                    continue;
                }
            };
            if args.is_empty() {
                continue;
            }

            match self.execute_command(&args).await {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) if error.to_string() == INCORRECT_COMMAND => incorrect_command(),
                Err(error) => println!(
                    "\n\nВиключення: {error:#}\n"
                ),
            }
        }

        self.cancel().await;
    }

    fn initialize_api(&mut self) {
        let api = Arc::new(ChatApi::new());
        api.set_response_timeout(10_000);
        self.api = Some(api);
    }

    fn safe_get_api(&self) -> Result<Arc<ChatApi>> {
        self.api
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("API is not initialized yet"))
    }

    fn start_collect_messages(&mut self) {
        if let Some(jh) = self.collect_join_handle.take() {
            jh.abort();
        }

        let Some(api) = self.api.as_ref().cloned() else {
            return;
        };

        let mut events = api.subscribe_events();

        self.collect_join_handle = Some(tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(ChatEvent::MessageReceived { sender_id, text }) => {
                        let sender_name = match api
                            .request_user_by_id(sender_id)
                            .await
                        {
                            Ok(name) => format!("{name} ({sender_id})"),
                            Err(error) => {
                                eprintln!(
                                    "Не вдалося отримати ім'я відправника \
                                 {sender_id}: {error:#}"
                                );
                                sender_id.to_string()
                            }
                        };

                        println!(
                            "\nОтримано повідомлення:\nsender: {sender_name}\ntext: {text}\n"
                        );
                    }

                    Ok(ChatEvent::DeliveryConfirmed) => {
                        println!("\nПовідомлення доставлено\n");
                    }

                    Ok(ChatEvent::ConnectionClosed) => {
                        println!("\nЗ'єднання із сервером закрито\n");
                    }

                    Ok(ChatEvent::Error(message)) => {
                        println!("\nПомилка: {message}\n");
                    }

                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }

                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }));
    }

    async fn cancel(&mut self) {
        if let Some(job) = self.collect_join_handle.take() {
            job.abort();
        }
        if let Some(api) = self.api.take() {
            let _ = api.disconnect().await;
        }
    }

    async fn restart(&mut self) {
        self.cancel().await;
        self.recipient_id = None;
        self.initialize_api();
        self.start_collect_messages();
    }


    async fn send_message_to_all(&self, message: &str) -> Result<()> {
        let api = self.safe_get_api()?;

        let active_users = api.get_active_users().await?;
        let current_user_id = api.current_user().await.map(|user| user.id);

        let mut recipients: Vec<_> = active_users
            .values()
            .filter(|user| Some(user.id) != current_user_id)
            .map(|user| (user.id, user.name.clone()))
            .collect();

        recipients.sort_by_key(|(id, _)| *id);

        if recipients.is_empty() {
            println!("Немає інших активних користувачів для надсилання");
            return Ok(());
        }

        let total = recipients.len();
        let mut sent = 0;
        let mut failures = Vec::new();

        for (user_id, user_name) in recipients {
            match api.send_message(user_id, message).await {
                Ok(()) => {
                    sent += 1;
                }

                Err(error) => {
                    failures.push(format!(
                        "{user_name} ({user_id}): {error:#}"
                    ));
                }
            }
        }

        println!(
            "Результат надсилання: {sent}/{total} успішно"
        );

        if !failures.is_empty() {
            return Err(anyhow!(
            "Не вдалося надіслати повідомлення деяким користувачам: {}",
            failures.join("; ")
        ));
        }

        Ok(())
    }

    async fn execute_command(&mut self, args: &[String]) -> Result<bool> {
        match args[0].to_lowercase().as_str() {
            "connect" => {
                check_count(args, 5)?;
                if args[1] != "-host" || args[3] != "-port" {
                    return incorrect_result();
                }
                let port = args[4]
                    .parse::<u16>()
                    .map_err(|_| anyhow!("{INCORRECT_COMMAND}"))?;
                self.safe_get_api()?.connect(&args[2], port).await?;
                println!("Підключено до {}:{}", args[2], port);
            }
            "disconnect" => {
                check_count(args, 1)?;
                self.safe_get_api()?.disconnect().await?;
                println!("Відключено від сервера");
            }
            "login" => {
                check_count(args, 3)?;
                if args[1] != "-name" {
                    return incorrect_result();
                }
                self.safe_get_api()?.login(&args[2]).await?;
                if let Some(user) = self.safe_get_api()?.current_user().await {
                    println!("Виконано вхід: {} ({})", user.name, user.id);
                } else {
                    println!("Вхід виконано");
                }
            }
            "logout" => {
                check_count(args, 1)?;
                self.safe_get_api()?.logout().await?;
                println!("Вихід виконано");
            }
            "getactiveusers" => {
                check_count(args, 1)?;

                let active_users = self
                    .safe_get_api()?
                    .get_active_users()
                    .await?;

                if active_users.is_empty() {
                    println!("Немає активних користувачів");
                } else {
                    println!("Активні користувачі:");

                    let mut values: Vec<_> = active_users.values().collect();
                    values.sort_by_key(|user| user.id);

                    for user in values {
                        println!("id={}, name={}", user.id, user.name);
                    }
                }
            }
            "sendmessage" => {
                match args.len() {
                    5 => {
                        if args[1] != "-recipientId" || args[3] != "-message"
                        { return incorrect_result(); }

                        let recipient_id = args[2]
                            .parse::<i64>()
                            .map_err(|_| anyhow!("{INCORRECT_COMMAND}"))?;

                        self.safe_get_api()?
                            .send_message(recipient_id, &args[4])
                            .await?;
                    }

                    3 => {
                        if args[1] != "-message" { return incorrect_result(); }

                        let recipient_id = self
                            .recipient_id
                            .ok_or_else(|| anyhow!("Отримувач не встановлений"))?;

                        self.safe_get_api()?
                            .send_message(recipient_id, &args[2])
                            .await?;
                    }
                    4 => {
                        if args[1] != "-all" || args[2] != "-message" { return incorrect_result(); }
                        self.send_message_to_all(&args[3]).await?;
                    }
                    _ => return incorrect_result(),
                }
            }
            "setrecipient" => {
                check_count(args, 3)?;

                match args[1].as_str() {
                    "-recipientId" => {
                        let id = args[2]
                            .parse::<i64>()
                            .map_err(|_| anyhow!("{INCORRECT_COMMAND}"))?;

                        let user_name = self
                            .safe_get_api()?
                            .request_user_by_id(id)
                            .await?;

                        self.recipient_id = Some(id);

                        println!(
                            "Отримувач встановлений: {user_name} ({id})"
                        );
                    }

                    "-name" => {
                        let active_users = self
                            .safe_get_api()?
                            .get_active_users()
                            .await?;

                        let user = active_users
                            .values()
                            .find(|user| user.name == args[2]);

                        let Some(user) = user else {
                            return Err(anyhow!(
                           "Активного користувача з ім'ям '{}' не знайдено",
                            args[2]));
                        };

                        let id = user.id;
                        let name = user.name.clone();

                        self.recipient_id = Some(id);

                        println!(
                            "Отримувач встановлений: {name} ({id})"
                        );
                    }

                    _ => return incorrect_result(),
                }
            }
            "resetrecipient" => {
                check_count(args, 1)?;
                self.recipient_id = None;
                println!("Отримувач скинутий");
            }
            "resetapi" => {
                check_count(args, 1)?;
                self.restart().await;
                println!("API перезапущено");
            }
            "help" => {
                check_count(args, 1)?;
                print_help();
            }
            "status" => {
                check_count(args, 1)?;
                let Some(api) = self.api.as_ref() else {
                    println!("API не ініціалізовано");
                    return Ok(false);
                };
                println!("API ініціалізовано");
                println!(
                    "Стан з'єднання: {}",
                    if api.is_connected().await { "підключено" } else { "не підключено" }
                );
                if let Some(user) = api.current_user().await {
                    println!("Користувач: {} ({})", user.name, user.id);
                }
            }
            "exit" => {
                check_count(args, 1)?;
                return Ok(true);
            }
            _ => incorrect_result()?,
        }
        Ok(false)
    }
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

fn incorrect_command() {
    println!("Команда некоректна");
}

fn parse_args(input: &str) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut token_started = false;

    for ch in input.chars() {
        match ch {
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

    if in_quotes {
        return Err(anyhow!("Незакриті лапки"));
    }
    if token_started {
        args.push(current);
    }
    Ok(args)
}

fn print_help() {
    println!(
        r#"Доступні команди:

----- підключення -----
connect -host "host" -port "port"   Підключення до сервера
disconnect                          Відключення від сервера

----- авторизація -----
login -name "username"              Вхід до чату
logout                              Вихід з облікового запису

----- користувачі -----
getActiveUsers                      Отримання активних користувачів

----- відправка повідомлень -----
sendMessage -recipientId "id" -message "message"
                                    Надсилання користувачу за ID
sendMessage -message "message"      Надсилання поточному отримувачу
sendMessage -all -message "message" Надсилання всім іншим активним користувачам

----- встановлення отримувача -----
setRecipient -recipientId "id"      Встановлення за ID
setRecipient -name "username"       Встановлення за ім'ям
resetRecipient                      Скасування отримувача

----- API -----
status                              Перевірка стану API
resetApi                            Перезапуск API

----- інше -----
help                                Показати довідку
exit                                Завершити роботу"#
    );
}
