use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use merged_orderbook::api;
use std::{io, time::Duration};
use tokio::sync::mpsc;
use tokio::{select, sync::watch};
use tokio_stream::StreamExt;
use tonic::transport::Endpoint;
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    text::{Span, Spans},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame, Terminal,
};

#[tokio::main]
async fn main() {
    let args: Cli = Cli::parse();
    let endpoint = args
        .endpoint
        .unwrap_or_else(|| "http://[::1]:50051".parse().unwrap());

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
    let (orderbook_tx, orderbook_rx) = watch::channel(Err("".to_string()));

    // start ui task
    tokio::spawn(async move {
        if let Err(e) = run_ui_loop(shutdown_tx.clone(), orderbook_rx).await {
            eprintln!("Could not setup terminal ui: {}", e);
            shutdown_tx
                .send(())
                .await
                .expect("shutdown receiver should not be closed");
            return;
        }
    });

    // connect to server
    select! {
        _ = run_connect_loop(endpoint, orderbook_tx) => {},
        _ = shutdown_rx.recv() => {},
    };
}

#[derive(Parser)]
#[clap(author, version, about = "A simple client that just calls the accompanying server", long_about = None)]
pub struct Cli {
    /// The endpoint to connect to (default: "http://[::1]:50051").
    endpoint: Option<tonic::transport::Endpoint>,
}

async fn run_connect_loop(
    endpoint: Endpoint,
    orderbook_tx: watch::Sender<Result<api::Summary, String>>,
) {
    loop {
        let mut client = match api::orderbook_aggregator_client::OrderbookAggregatorClient::connect(
            endpoint.clone(),
        )
        .await
        {
            Ok(client) => client,
            Err(e) => {
                orderbook_tx
                    .send(Err(format!(
                        "Error connecting to orderbook aggregator {}",
                        e
                    )))
                    .expect("orderbook receiver should not be dropped");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        if let Ok(r) = client.book_summary(api::Empty {}).await {
            let mut stream = r.into_inner();
            while let Some(result) = stream.next().await {
                match result {
                    Ok(item) => {
                        // println!("{:?}", item);
                        orderbook_tx
                            .send(Ok(item))
                            .expect("orderbook receiver should not be dropped");
                    }
                    Err(e) => orderbook_tx
                        .send(Err(format!("Error receiving data: {}", e)))
                        .expect("orderbook receiver should not be dropped"),
                }
            }
        }
    }
}

async fn run_ui_loop(
    shutdown: mpsc::Sender<()>,
    orderbook_rx: watch::Receiver<Result<api::Summary, String>>,
) -> Result<(), std::io::Error> {
    // setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    loop {
        {
            let data = orderbook_rx.borrow();
            terminal.draw(|f| ui(f, &data))?;
        }
        if event::poll(Duration::from_secs(0))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char(c) => {
                        if c == 'q' {
                            break; // leave loop to clean up and send shutdown signal
                        }
                    }
                    _ => {}
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    // send shutdown signal
    shutdown.send(()).await.expect("failed to shutdown");

    Ok(())
}

fn ui<B: Backend>(f: &mut Frame<B>, state: &Result<api::Summary, String>) {
    let main = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(12),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(f.size());
    let lists = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
        .split(main[0]);
    let ask_block = Block::default()
        .title("Ask (price, qty, exchange)")
        .borders(Borders::ALL);
    // f.render_widget(ask_block, lists[0]);
    let bid_block = Block::default()
        .title("Bid (price, qty, exchange)")
        .borders(Borders::ALL);
    // f.render_widget(bid_block, lists[1]);

    /// Formats the given levels as a list
    fn list_levels(levels: &[api::Level], style: Style) -> Vec<ListItem> {
        levels
            .iter()
            .map(|l| {
                let content = Spans::from(vec![Span::styled(
                    format!("{:<10} {:<10} {}", l.price, l.amount, l.exchange),
                    style,
                )]);
                ListItem::new(content)
            })
            .collect()
    }

    let ask_style = Style::default().fg(Color::Red);
    let bid_style = Style::default().fg(Color::Green);
    match state {
        Ok(summary) => {
            let asks = list_levels(&summary.asks, ask_style);
            let bids = list_levels(&summary.bids, bid_style);
            let asks_list = List::new(asks).block(ask_block);
            let bids_list = List::new(bids).block(bid_block);
            f.render_widget(asks_list, lists[0]);
            f.render_widget(bids_list, lists[1]);

            let spread_text = Paragraph::new(format!("Spread: {}", summary.spread));
            f.render_widget(spread_text, main[1]);
        }
        Err(error) => {
            let status = Paragraph::new(error.as_ref());
            f.render_widget(status, main[1]);
        }
    }
    let footer = Paragraph::new("Press 'q' to quit.");
    f.render_widget(footer, main[2]);
}
