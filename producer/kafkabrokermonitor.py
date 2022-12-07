
import smtplib
from email.message import EmailMessage
import os
import logging
import schedule
import time
from kafka import KafkaProducer
import json
import functools





KAFKA_BOOTSTRAP_SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVER')

# logger
logger = logging.getLogger('monitorkafkalogger')
hdlr = logging.FileHandler('monitorkafkalog.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def catch_exceptions(cancel_on_failure=False):
    def catch_exceptions_decorator(job_func):
        @functools.wraps(job_func)
        def wrapper(*args, **kwargs):
            try:
                return job_func(*args, **kwargs)
            except:
                import traceback
                print(traceback.format_exc())
                if cancel_on_failure:
                    return schedule.CancelJob
        return wrapper
    return catch_exceptions_decorator



def callkafkaproducer():

    errorMessage = ""
    exceptionMessage =""
    isErrorKAFKA=False
    producer = None
    try:
        ## Producer
        logger.info("Start : Creation du KafkaProducer with all brokers ")
        # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
        # request_timeout_ms (int) – Client request timeout in milliseconds. Default: 30000.
        #max_block_ms (int) – Number of milliseconds to block during send() and partitions_for(). These methods can be blocked either because the buffer is full or metadata unavailable. 
        #Blocking in the user-supplied serializers or partitioner will not be counted against this timeout. Default: 60000.
        
        producer = KafkaProducer(request_timeout_ms=60000,max_block_ms=120000,bootstrap_servers=['localhost:9092','localhost:9093'],value_serializer=lambda v: json.dumps(v).encode('utf-8'),max_request_size=20971520,api_version = (2, 8, 0))
  
        # Publish /Send
        producer.send(topic='supervisionmonitoring',key= b'MTN', value={'refContrat': '255358642'})
        #produder.flush()
        #metadata= ack.get()
        logger.info("Publish ... to topic supervisionmonitoring.... ")
        print("Publish ... to topic supervisionmonitoring....")
  
       
    except KafkaTimeoutError as kte:
            print("----------Exeption --------")
            isErrorKAFKA=True
            errorMessage = "TimeoutError sending message to Kafka/Kafkaproducer "
            exceptionMessage =str(kte)
            logger.exception( errorMessage+ ": %s", kte)
            raise
            
    except KafkaError as ke:
            print("----------Exeption --------")
            isErrorKAFKA=True
            errorMessage = "KafkaError sending message to Kafka /Kafkaproducer "
            exceptionMessage =str(ke)
            logger.exception(errorMessage+ " %s", ke)
            raise
         
    except Exception as e:
            print("----------Exeption --------")
            isErrorKAFKA=True
            errorMessage = "Kafka Exception sending message to Kafka/Kafkaproducer "
            exceptionMessage =str(e)
            logger.exception(errorMessage+ " %s", e)
            raise

    finally:
        # https://stackoverflow.com/questions/34249269/finally-and-rethowing-of-exception-in-except-raise-in-python
        # TODO
        if producer is None:
            sendEmailAlerte("KafkaTimeoutError /Broker", "Broker x not Available") 
        #elif producer is not None:
        else:
            logger.info("RAS : Test du Broker Kafka OK ....")  
        
        
    # Close
    producer.close()
    


def sendEmailAlerte(errorMessage, exceptionMessage):
    logger.info(" Begin: Call Email Sender Alert ....************************************")   
    msg = EmailMessage()
    #msg.set_content('This is my message')
    fullexceptionmessage = "Bonjour à Toutes et à Tous,  </br> </br> Alerte sur la disponibilité des brokers KAFKA.</br> Message = Error after call Kafkaproducer : <p>" + exceptionMessage + "</p> </br></br>  Cordialement"
    msg.set_content(fullexceptionmessage, subtype='html')

    msg['Subject'] = errorMessage
    msg['From'] = "me@gmail.com"
    msg['To'] = "you@gmail.com"

    try:
        # Send the message via our own SMTP server.
        server = smtplib.SMTP('localhost', 25)
        #server.login("me@gmail.com", "password")
        #server.starttls()
        server.send_message(msg)
        logger.info("OK: Send email ...")
        
    except  Exception as e:
      print("Error: unable to send email : Print message ...", str(e))
      logger.error(f"Error: unable to send email: {e}")
    else:
      #print("Nothing went wrong")
      server.quit()

def sendEmailHeartBeat():
    msg = EmailMessage()
    #msg.set_content('This is my message')
  
    fullexceptionmessage = " Bonjour à Toutes et à Tous,  </br> </br> Message = HeartBeat for Kafka monitoring tools: Up ...</br> </br>  Cordialement"
    msg.set_content(fullexceptionmessage,subtype='html')

    msg['Subject'] = "HeartBeat Kafka Broker Monitoring"
    msg['From'] = "me@gmail.com"
    msg['To'] = "you@gmail.com"

    try:
        # Send the message via our own SMTP server.
        server = smtplib.SMTP('localhost', 25)
        #server.login("me@gmail.com", "password")
        #server.starttls()
        server.send_message(msg)
        logger.info("OK: Send email ...")
        
    except  Exception as e:
        print("Error: unable to send email : Print message ...", str(e))
        logger.error(f"Error: unable to send email: {e}")
        raise
    else:
        #print("Nothing went wrong")
        server.quit()



@catch_exceptions(cancel_on_failure=True)
def  doTaskKafka():
    print("--- Begin: call do Task Kafka  ----")
    try:
        callkafkaproducer()
        print("--- End of Task Kafka Broker :  OK ----")
    except Exception as e:
            errorMessage = "Kafka Exception sending message to Kafka/Kafkaproducer"
            logger.exception(errorMessage+ " %s", e)


@catch_exceptions(cancel_on_failure=True)
def  doTaskEmailHeartBeat():
    print("--- Begin: call do Task Email HeartBeat  ----")
    try:
        callkafkaproducer()
        print("--- End of Task Kafka Email HeartBeat :  OK ----")
    except Exception as e:
            errorMessage = "Kafka Exception sending email /heartbeat "
            logger.exception(errorMessage+ " %s", e)    
    
def initScheduleTask():
    # for every n minutes
    schedule.every(1).minutes.do(doTaskKafka)
   
    # every hour
    schedule.every().hour.do(sendEmailHeartBeat)

    """ Sample ScheduleTask
    # every daya at specific time
    schedule.every().day.at("10:30").do(task)

    # schedule by name of day
    schedule.every().monday.do(task)

    # name of day with time
    schedule.every().wednesday.at("13:15").do(task)
    """

    while True:
        schedule.run_pending()
        time.sleep(1) 

def main():
    print("Start  ScheduleTask  ...")
    initScheduleTask()
    

if __name__ == "__main__":
    main()
