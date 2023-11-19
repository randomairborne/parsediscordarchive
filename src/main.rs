use std::{fs::OpenOptions, path::PathBuf, time::Instant};

use chrono::Utc;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};

#[derive(serde::Serialize, Clone)]
struct Reply {
    prompt: String,
    reply: String,
}

#[derive(serde::Serialize, Clone)]
struct Message {
    id: u64,
    content: String,
    timestamp: chrono::DateTime<Utc>,
    author: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference: Option<u64>,
}

#[serde_as]
#[derive(serde::Deserialize)]
struct DiscordMessage {
    #[serde_as(as = "DisplayFromStr")]
    id: u64,
    content: String,
    timestamp: chrono::DateTime<Utc>,
    author: DiscordAuthor,
    message_reference: Option<DiscordMessageReference>,
}

#[serde_as]
#[derive(Debug, Deserialize)]
struct DiscordAuthor {
    #[serde_as(as = "DisplayFromStr")]
    id: u64,
}

#[serde_as]
#[derive(Debug, Deserialize)]
struct DiscordMessageReference {
    #[serde_as(as = "Option<DisplayFromStr>")]
    message_id: Option<u64>,
}

fn main() {
    let root_path = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("Expected first argument to be path"),
    );
    let who_string = std::env::args()
        .nth(2)
        .expect("Expected a second argument of a discord user id");
    let who: u64 = who_string.parse().unwrap();

    let out_file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(format!("./prompt-{who}.json"))
        .unwrap();

    let mut channels: Vec<Vec<Message>> = Vec::with_capacity(256);

    let channel_files = channel_files(root_path);
    let parse_start = Instant::now();
    let total_files = channel_files.len();
    let mut completed: usize = 0;

    for messages_json in channel_files {
        let our_number = completed;
        completed += 1;
        let start = Instant::now();
        println!("Starting parsing on {messages_json:?} ({our_number}/{total_files})");
        let file = OpenOptions::new().read(true).open(&messages_json).unwrap();
        let data: Vec<DiscordMessage> = simd_json::from_reader(file).unwrap();
        let mut messages: Vec<Message> = data
            .into_iter()
            .map(|v| Message {
                id: v.id,
                author: v.author.id,
                content: v.content,
                timestamp: v.timestamp,
                reference: v.message_reference.map(|v| v.message_id).flatten(),
            })
            .collect();
        messages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        channels.push(messages);
        let end = Instant::now();
        let duration = end - start;
        println!(
            "Completed parsing on {messages_json:?} ({our_number}/{total_files}), took {}ms",
            duration.as_millis()
        );
    }
    println!(
        "Completed all parsing in {} seconds, have {} messages from {} channels",
        (Instant::now() - parse_start).as_secs(),
        channels.iter().map(|v| v.len()).sum::<usize>(),
        channels.len()
    );
    let mut replies: Vec<Reply> = Vec::with_capacity(100_000);
    for channel in channels {
        for (index, message) in channel.iter().enumerate() {
            if message.author != who || message.content.is_empty() {
                continue;
            }
            let reply = message.content.clone();
            let Some(prompt) = get_prompt(&channel, index, who) else {
                continue;
            };
            replies.push(Reply { prompt, reply });
        }
    }
    serde_json::to_writer(out_file, &replies).unwrap();
    println!("Done, see ya!");
}

fn get_prompt(messages: &[Message], index: usize, who: u64) -> Option<String> {
    if index == 0 {
        return None;
    }
    let mut innerdex = index - 1;
    let reply = &messages[index];
    let mut outputs: Vec<String> = Vec::new();
    let mut reference_time = reply.timestamp;

    if let Some(reference) = reply.reference {
        for (reply_index, message) in messages[0..innerdex].iter().enumerate() {
            if message.id == reference {
                innerdex = reply_index;
                reference_time = message.timestamp;
                break;
            }
        }
    }
    while innerdex != 0
        && outputs.len() < 5
        && messages[innerdex].author != who
        && (messages[innerdex].timestamp - reference_time).num_minutes() <= 10
    {
        let prompt = &messages[innerdex];
        innerdex -= 1;
        if prompt.content.is_empty() {
            continue;
        }
        outputs.push(prompt.content.clone());
    }
    if outputs.is_empty() {
        None
    } else {
        outputs.reverse();
        Some(outputs.join("\n"))
    }
}

fn walkdir(path: PathBuf) -> Vec<PathBuf> {
    path.read_dir()
        .unwrap()
        .map(|v| v.unwrap().path())
        .collect()
}

fn channel_files(root_path: PathBuf) -> Vec<PathBuf> {
    let channel_dirs = walkdir(root_path);
    let mut thread_dirs = Vec::with_capacity(1024);
    let mut files = Vec::with_capacity(1024);

    for dir in &channel_dirs {
        let threads_dir = dir.join("threads");
        if threads_dir.exists() {
            let mut dirs = walkdir(threads_dir);
            thread_dirs.append(&mut dirs);
        } else {
            eprintln!("Found no threads in {dir:?}, skipping..");
        }
    }

    for dir in &channel_dirs {
        let messages_path = dir.join("channel_messages.json");
        if messages_path.exists() {
            files.push(messages_path);
        } else {
            eprintln!("Found no channel_messages.json in {dir:?}, skipping..");
        }
    }

    for dir in thread_dirs {
        let messages_path = dir.join("thread_messages.json");
        if messages_path.exists() {
            files.push(messages_path);
        } else {
            eprintln!("Found no thread_messages.json in {dir:?}, skipping..");
        }
    }
    files
}
