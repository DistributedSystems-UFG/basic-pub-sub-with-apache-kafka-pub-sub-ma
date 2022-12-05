from kafka import KafkaProducer
from random import seed
from random import randint
from datetime import datetime
import time
from const import *
import sys

"""
try:
    topic = sys.argv[1]
except:
    print ('Usage: python3 producer <topic_name>')
    exit(1)
"""

"""Simulando topicos de notificação de um equipamento que monitora a cada segundo as condições
 de uma sala de servidores e a disponibilidade da rede elétrica"""
topicos = {"temperatura": ["Alta", "Baixa", "Normal"]
    , "umidade": ["Alta", "Baixa", "Normal"]
    , "tensao": ["Normal", "Instável", "Sem energia"]
    }

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

while True:
    for t in topicos.keys():
        now = datetime.now()
        estado = randint(0, 2)
        msg = now.strftime("%d/%m/%Y, %H:%M:%S") + ' - ' + t + ': ' + topicos[t][estado]
        print ('Sending message: ' + msg)
        producer.send(t, value=msg.encode())
        
    time.sleep(1)

producer.flush()
