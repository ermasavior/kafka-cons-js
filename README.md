# Kafka Topic Log Consumer

Kafka topic log consumer implemented using Node.js.

## Requirements

1. [Npm](https://www.npmjs.com/get-npm)
2. [Protoc](http://google.github.io/proto-lens/installing-protoc.html)

## Dependencies

1. kafka-node
2. google-protobuf

## Usage

In order to consume topics, you have to specify which kafka host, protobuf schema, and topic to subscribe.

### How to Run
1. Install dependencies
    ```
    npm install
    ```
2. Prepare your protobuf schema and compile it to js file

    Example:
    ```
    protoc --js_out=import_style=commonjs,binary:. \
    proto/types/Location.proto \
    proto/types/BookingStatus.proto \
    proto/booking/BookingLog.proto
    ```
3. Customize environment variables to `.env` file
    ```
    cp .env.sample .env
    ```
4. Run kafka consumer by executing
    ```
    node consumer.js
    ```
5. Logs consumed will be saved on `/output` dir.

## Author

Erma
erma.safira@gmail.com