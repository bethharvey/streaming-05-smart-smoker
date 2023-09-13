# streaming-05-smart-smoker

* Beth Harvey
* Streaming Data
* Module 5
* September 13, 2023

## Project Overview

The goal of this project is to create a producer to send messages representing three temperature readings from a smart smoker. Temp 1 is the temperature of the smoker itself, temp 2 is the temperature of one food (Food A), and temp 3 is the temperature of a second food (Food B). Each temperature is sent to a different consumer through a dedicated queue using RabbitMQ.

## Data Source

The temperature measurements for this project are individual rows from the [smoker-temps](smoker-temps.csv) file in this repository.

## Requirements

1. Git
2. Python 3.7+ (3.11+ preferred)
3. VS Code Editor
4. VS Code Extension: Python (by Microsoft)
5. RabbitMQ Server installed and running locally
6. Virtual Environment
    * `python3 -m venv .venv`
    * `source .venv/bin/activate`
7. Pika
    * `python3 -m pip install pika`