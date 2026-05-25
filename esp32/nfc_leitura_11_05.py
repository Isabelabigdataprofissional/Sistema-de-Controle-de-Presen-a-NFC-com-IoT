import machine
import utime
import network
from pn532 import PN532Uart
from umqtt.simple import MQTTClient

# ---------------- WIFI ----------------

ssid = "wifi"
wifi_senha = "jesuscristo"

wifi = network.WLAN(network.STA_IF)
wifi.active(True)
wifi.connect(ssid, wifi_senha)

print("Conectando no Wi-Fi...")

while not wifi.isconnected():
    utime.sleep_ms(500)

print("Conectado no WiFi")
print("IP:", wifi.ifconfig()[0])

# ---------------- MQTT ----------------

broker = "brw.net.br"
usuario = "brware"
mqtt_senha = "SQRT(pi)!=314"

client = MQTTClient(
    client_id="esp32",
    server=broker,
    user=usuario,
    password=mqtt_senha,
    port=1883
)

client.connect()

print("Conectado ao servidor MQTT")

# ---------------- NFC ----------------

DEBUG = False

led = machine.Pin(2, machine.Pin.OUT)
led.off()

rf = PN532Uart(2, tx=17, rx=16, debug=False)
rf.SAM_configuration()

ic, ver, rev, support = rf.get_firmware_version()

print("Found PN532 with firmware version: {0}.{1}".format(ver, rev))
print("NFC pronto")
print("Aproxime um cartão...")

# ---------------- CONTROLE ----------------

ultimo_uid_publicado = None
ultimo_tempo_publicado = 0

# 5 segundos
INTERVALO_REPETICAO = 2000

# ---------------- LOOP PRINCIPAL ----------------

while True:
    try:
        uid = rf.read_passive_target()

        if uid is not None:
            uid_str = " ".join("{:02X}".format(x) for x in uid)

            agora = utime.ticks_ms()

            mesmo_cartao = uid_str == ultimo_uid_publicado

            passou_5s = (
                utime.ticks_diff(agora, ultimo_tempo_publicado)
                >= INTERVALO_REPETICAO
            )

            # Publica se:
            # - for cartão diferente
            # OU
            # - já passaram 5 segundos

            if (not mesmo_cartao) or passou_5s:

                print("Card UUID:", uid_str)

                client.publish("aluno/id", uid_str)

                print("Publicado no MQTT:", uid_str)

                led.on()
                utime.sleep_ms(200)
                led.off()

                ultimo_uid_publicado = uid_str
                ultimo_tempo_publicado = agora

        utime.sleep_ms(300)

    except Exception:

        try:
            rf.release_targets()
        except:
            pass

        utime.sleep_ms(300)