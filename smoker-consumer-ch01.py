"""
    This program listens for work messages contiously. 
    If the temperature of the smoker decreases by more than 15 degrees F in 2.5 minutes, a smoker alert is sent.

    Author: Beth Harvey
    Date: September 24, 2023

"""

import pika
import sys
from collections import deque

# Import function to send email alerts
from email_alerts import createAndSendEmailAlert

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# declare deque length
SMOKER_DEQUE = deque(maxlen=5)


# define a callback function to be called when a message is received
def smoker_callback(ch, method, properties, body):
    """
    Define behavior on getting a message.
    Receives a message from the queue,
    extracts temperature from the message,
    checks the temperature difference over a given time period,
    and sends an alert if the temperature drops more than 15 degrees.
    """

    # decode the binary message body to a string
    logger.info(f" [x] Received {body.decode()}")

    try:
        # process smoker message
        smoker_mess = body.decode().split(",")
        # check to see if message contains a valid temperature to add to deque
        if smoker_mess[1] != "No temperature":
            smoker_temp = float(smoker_mess[1])
            smoker_timestamp = smoker_mess[0]
            SMOKER_DEQUE.append(smoker_temp)

            # calculate difference in temp between most recent temp and all previous temps
            if len(SMOKER_DEQUE) > 1:
                temp_changes = [
                    SMOKER_DEQUE[i] - SMOKER_DEQUE[-1]
                    for i in range(0, (len(SMOKER_DEQUE) - 1), 1)
                ]

                # check if any temperature changes are greater than 15 degrees
                if any(value > 15 for value in temp_changes):
                    logger.info(
                        f"Smoker alert at {smoker_timestamp}! Smoker temperature has dropped more than 15 degrees."
                    )
                    email_subject = "Smoker Alert!"
                    email_body = f"Smoker alert at {smoker_timestamp}! The smoker temperature has dropped more than 15 degrees in 2.5 minutes."
                    createAndSendEmailAlert(email_subject, email_body)

        # when done with task, tell the user
        logger.info(" [x] Processed smoker temp.")
        # acknowledge the message was received and processed
        # (now it can be deleted from the queue)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error("An error has occurred with the smoker message.")
        logger.error(f"The error says: {e}.")


# define a main function to run the program
def main(hn: str = "localhost", qn: str = "task_queue"):
    """Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        logger.error("ERROR: connection to RabbitMQ server failed.")
        logger.error(f"Verify the server is running on host={hn}.")
        logger.error(f"The error says: {e}")
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance.
        # prefetch_count = Per consumer limit of unaknowledged messages
        channel.basic_qos(prefetch_count=1)

        # configure the channel to listen on a specific queue,
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(
            queue=qn, on_message_callback=smoker_callback, auto_ack=False
        )

        # print a message to the console for the user
        logger.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.error("ERROR: something went wrong.")
        logger.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "01-smoker")
