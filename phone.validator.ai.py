import csv
import os
import time
import logging
from datetime import datetime
from typing import List, Dict, Tuple
import requests
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
import openai

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('phone_validation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PhoneValidator:
    def __init__(self):
        # Twilio налаштування
        self.twilio_account_sid = os.getenv('TWILIO_ACCOUNT_SID')
        self.twilio_auth_token = os.getenv('TWILIO_AUTH_TOKEN')
        self.twilio_phone_number = os.getenv('TWILIO_PHONE_NUMBER')  # Ваш Twilio номер
        
        # OpenAI налаштування
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        
        # Ініціалізація клієнтів
        self.twilio_client = Client(self.twilio_account_sid, self.twilio_auth_token)
        openai.api_key = self.openai_api_key
        
        # Фрази для визначення невалідних номерів
        self.invalid_phrases = [
            'недоступний', 'номер не обслуговується', 'невірно набраний',
            'поза зоною', 'абонент відсутній', 'номер відключений',
            'неправильний номер', 'не існує', 'тимчасово недоступний',
            'номер заблокований', 'послуга недоступна'
        ]
        
        # Пауза між дзвінками (секунди)
        self.call_delay = 2
        
        # Тривалість запису (секунди)
        self.recording_duration = 10
        
        self._validate_credentials()
    
    def _validate_credentials(self):
        """Перевірка наявності API ключів"""
        required_vars = [
            'TWILIO_ACCOUNT_SID', 'TWILIO_AUTH_TOKEN', 
            'TWILIO_PHONE_NUMBER', 'OPENAI_API_KEY'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Відсутні змінні середовища: {', '.join(missing_vars)}")
    
    def read_phone_numbers(self, csv_file: str) -> List[str]:
        """Читання номерів з CSV файлу"""
        phones = []
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if 'phone' in row and row['phone'].strip():
                        phones.append(row['phone'].strip())
            
            logger.info(f"Завантажено {len(phones)} номерів з {csv_file}")
            return phones
        
        except FileNotFoundError:
            logger.error(f"Файл {csv_file} не знайдено")
            return []
        except Exception as e:
            logger.error(f"Помилка читання файлу: {e}")
            return []
    
    def make_call_with_recording(self, phone_number: str) -> Tuple[str, str]:
        """
        Здійснення дзвінка з записом
        Повертає: (call_sid, recording_sid)
        """
        try:
            # TwiML для запису дзвінка
            twiml_url = f"http://twimlets.com/holdmusic?Bucket=com.twilio.music.ambient"
            
            # Здійснення дзвінка
            call = self.twilio_client.calls.create(
                to=phone_number,
                from_=self.twilio_phone_number,
                url=twiml_url,
                record=True,
                recording_channels='mono',
                recording_status_callback_event=['completed'],
                timeout=self.recording_duration,
                method='GET'
            )
            
            logger.info(f"Дзвінок розпочато: {phone_number} (SID: {call.sid})")
            
            # Очікування завершення дзвінка
            time.sleep(self.recording_duration + 2)
            
            # Отримання запису
            recordings = self.twilio_client.recordings.list(call_sid=call.sid, limit=1)
            recording_sid = recordings[0].sid if recordings else None
            
            return call.sid, recording_sid
            
        except TwilioRestException as e:
            logger.error(f"Помилка Twilio для {phone_number}: {e}")
            return None, None
        except Exception as e:
            logger.error(f"Загальна помилка для {phone_number}: {e}")
            return None, None
    
    def download_recording(self, recording_sid: str) -> str:
        """Завантаження аудіозапису"""
        if not recording_sid:
            return None
        
        try:
            recording = self.twilio_client.recordings(recording_sid).fetch()
            audio_url = f"https://api.twilio.com{recording.uri.replace('.json', '.mp3')}"
            
            # Завантаження файлу
            response = requests.get(audio_url, auth=(self.twilio_account_sid, self.twilio_auth_token))
            
            if response.status_code == 200:
                filename = f"recording_{recording_sid}.mp3"
                with open(filename, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"Запис завантажено: {filename}")
                return filename
            else:
                logger.error(f"Не вдалося завантажити запис {recording_sid}")
                return None
                
        except Exception as e:
            logger.error(f"Помилка завантаження запису: {e}")
            return None
    
    def transcribe_audio(self, audio_file: str) -> str:
        """Розпізнавання мови через OpenAI Whisper"""
        if not audio_file or not os.path.exists(audio_file):
            return ""
        
        try:
            with open(audio_file, 'rb') as f:
                transcript = openai.Audio.transcribe(
                    model="whisper-1",
                    file=f,
                    language="uk"  # Українська мова
                )
            
            text = transcript.get('text', '').lower()
            logger.info(f"Розпізнано текст: {text[:100]}...")
            
            # Видалення тимчасового файлу
            os.remove(audio_file)
            
            return text
            
        except Exception as e:
            logger.error(f"Помилка розпізнавання: {e}")
            return ""
    
    def is_valid_number(self, transcribed_text: str) -> bool:
        """Перевірка валідності номера на основі розпізнаного тексту"""
        if not transcribed_text:
            return False
        
        text_lower = transcribed_text.lower()
        
        # Перевірка на наявність фраз невалідності
        for phrase in self.invalid_phrases:
            if phrase in text_lower:
                return False
        
        # Якщо текст занадто короткий (менше 3 символів), вважаємо невалідним
        if len(transcribed_text.strip()) < 3:
            return False
        
        return True
    
    def validate_phone_number(self, phone_number: str) -> Dict:
        """Повна перевірка одного номера"""
        logger.info(f"Перевірка номера: {phone_number}")
        
        # Здійснення дзвінка
        call_sid, recording_sid = self.make_call_with_recording(phone_number)
        
        if not call_sid:
            return {
                'phone': phone_number,
                'status': 'ERROR',
                'transcribed_text': 'Помилка дзвінка',
                'call_sid': None,
                'timestamp': datetime.now().isoformat()
            }
        
        # Завантаження запису
        audio_file = self.download_recording(recording_sid)
        
        # Розпізнавання тексту
        transcribed_text = self.transcribe_audio(audio_file) if audio_file else ""
        
        # Визначення валідності
        is_valid = self.is_valid_number(transcribed_text)
        status = 'VALID' if is_valid else 'INVALID'
        
        result = {
            'phone': phone_number,
            'status': status,
            'transcribed_text': transcribed_text,
            'call_sid': call_sid,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Результат для {phone_number}: {status}")
        return result
    
    def process_phone_list(self, input_csv: str, output_csv: str):
        """Обробка списку номерів"""
        phones = self.read_phone_numbers(input_csv)
        
        if not phones:
            logger.error("Немає номерів для обробки")
            return
        
        results = []
        
        # Створення заголовків для вихідного файлу
        fieldnames = ['phone', 'status', 'transcribed_text', 'call_sid', 'timestamp']
        
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for i, phone in enumerate(phones, 1):
                logger.info(f"Обробка {i}/{len(phones)}: {phone}")
                
                result = self.validate_phone_number(phone)
                results.append(result)
                
                # Запис результату відразу
                writer.writerow(result)
                csvfile.flush()  # Примусовий запис на диск
                
                # Пауза між дзвінками
                if i < len(phones):
                    logger.info(f"Пауза {self.call_delay} секунд...")
                    time.sleep(self.call_delay)
        
        # Статистика
        valid_count = sum(1 for r in results if r['status'] == 'VALID')
        invalid_count = sum(1 for r in results if r['status'] == 'INVALID')
        error_count = sum(1 for r in results if r['status'] == 'ERROR')
        
        logger.info(f"Обробка завершена:")
        logger.info(f"Валідні: {valid_count}")
        logger.info(f"Невалідні: {invalid_count}")
        logger.info(f"Помилки: {error_count}")
        logger.info(f"Результати збережено у {output_csv}")

def main():
    """Головна функція"""
    validator = PhoneValidator()
    
    # Файли
    input_file = "phone_numbers.csv"  # Вхідний файл з номерами
    output_file = f"validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Запуск обробки
    validator.process_phone_list(input_file, output_file)

if __name__ == "__main__":
    main()