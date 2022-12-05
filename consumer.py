from kafka import KafkaConsumer
from const import *
import sys

print('Simulando topicos de notificação de um equipamento que monitora a cada segundo as condições de uma sala de servidores e a disponibilidade da rede elétrica\n')
print('Qual informação deseja consumir?\n')
print('1 - Temperatura\n')
print('2 - Umidade\n')
print('3 - Tensão\n')
opcao = int(input('Opção: '))

topico = ''
topico = 'temperatura' if (opcao == 1) else ('umidade' if(opcao == 2) else ( 'tensao' if(opcao == 3) else ''))

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

consumer.subscribe([topico])
for msg in consumer:
    print(msg.value.decode())
