from kafka import KafkaConsumer
import json
import multiprocessing

stop_event = multiprocessing.Event()

def main():

    ## Consumer
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
    
    ##  Subscribe
    consumer.subscribe(['paiementMNPF'])
    
    while not stop_event.is_set():   
    
        for message in consumer:
            print(" Topic: " + str(message[0])
            + "\n Message: " + str(message[6], 'utf-8')
            + "\n Record: " + str(message))
            if stop_event.is_set():
                break
    consumer.close()

if __name__ == '__main__':
    main()
    
    
    


# consumer = KafkaConsumer(bootstrap_servers='localhost:9092')