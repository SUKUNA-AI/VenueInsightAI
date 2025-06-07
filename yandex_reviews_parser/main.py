import os
import random
import time
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import psycopg2

# Список User-Agent'ов
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.7151.69 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.7151.69 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]

# Параметры подключения к PostgreSQL
db_params = {
    "host": "db",
    "port": 5432,  # Внутренний порт в сети Docker
    "user": "postgres",
    "password": "qwerty12345",
    "database": "reviews_db",
    "client_encoding": "UTF8"
}

# Функция для вывода с временной меткой
def log_print(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# Функция для настройки базы данных
def setup_database():
    log_print("Проверка подключения и настройка базы данных...")
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS establishment_data (
                id SERIAL PRIMARY KEY,
                href TEXT,
                name TEXT,
                address TEXT,
                phone TEXT,
                rate TEXT,
                rate_count TEXT,
                site TEXT,
                average_bill TEXT
            );
            
            CREATE TABLE IF NOT EXISTS reviews (
                id SERIAL PRIMARY KEY,
                establishment_id INTEGER REFERENCES establishment_data(id),
                author TEXT,
                rating TEXT,
                review_text TEXT,
                date TEXT
            );
        """)
        conn.commit()
        cursor.execute("SELECT 1")
        conn.close()
        log_print("База данных настроена или уже существует.")
    except Exception as e:
        log_print(f"Ошибка подключения: {e}")
        raise

# Функция для сохранения данных о заведении
def save_establishment_to_db(data):
    log_print(f"Сохранение данных о заведении: {data}")
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO establishment_data (href, name, address, phone, rate, rate_count, site, average_bill)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get("href", "null"),
            data.get("name", "null"),
            data.get("address", "null"),
            data.get("phone", "null"),
            data.get("rate", "null"),
            data.get("rate_count", "null"),
            data.get("site", "null"),
            data.get("average_bill", "null")
        ))
        establishment_id = cursor.fetchone()[0]
        conn.commit()
        log_print(f"Данные о заведении сохранены. ID: {establishment_id}")
        return establishment_id
    except Exception as e:
        log_print(f"Ошибка сохранения заведения: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

# Функция для сохранения отзывов
def save_reviews_to_db(establishment_id, reviews_list):
    if not establishment_id:
        log_print("Пропуск сохранения отзывов: заведение не сохранено.")
        return
    log_print(f"Сохранение отзывов для заведения ID {establishment_id}")
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        for review in reviews_list:
            cursor.execute("""
                INSERT INTO reviews (establishment_id, author, rating, review_text, date)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                establishment_id,
                review.get("author", "N/A"),
                review.get("rating", "N/A"),
                review.get("text", "N/A"),
                review.get("date", "N/A")
            ))
        conn.commit()
        log_print(f"Сохранено {len(reviews_list)} отзывов для заведения ID {establishment_id}")
    except Exception as e:
        log_print(f"Ошибка сохранения отзывов: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

# Функция создания драйвера
def create_driver():
    options = Options()
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--referer=https://www.google.com")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    service = Service("/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    log_print(f"Chromedriver версия: {driver.capabilities['chrome']['chromedriverVersion']}")
    log_print(f"Chrome версия: {driver.capabilities['browserVersion']}")
    return driver

# Функция попытки решения капчи
def solve_captcha(driver):
    try:
        log_print("Проверка наличия капчи...")
        WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")
        
        captcha_elements = driver.find_elements(By.CSS_SELECTOR, "input.CheckboxCaptcha-Button")
        if not captcha_elements:
            log_print("Капча не обнаружена, продолжаем.")
            return True
        
        log_print("Капча обнаружена, пытаемся решить...")
        driver.execute_script("document.querySelector('div[data-type=\"checkbox\"]')?.click();")
        time.sleep(2)
        
        captcha_button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input.CheckboxCaptcha-Button"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", captcha_button)
        time.sleep(random.uniform(0.5, 1.0))
        captcha_button.click()
        log_print("Клик по кнопке капчи выполнен.")
        
        WebDriverWait(driver, 15).until_not(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input.CheckboxCaptcha-Button"))
        )
        log_print("Капча успешно решена.")
        time.sleep(random.uniform(5, 10))
        return True
    except Exception as e:
        log_print(f"Ошибка при обработке капчи: {str(e)}")
        try:
            timestamp = int(time.time())
            driver.save_screenshot(f"captcha_error_{timestamp}.png")
            with open(f"captcha_page_{timestamp}.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            log_print(f"Скриншот сохранён: captcha_error_{timestamp}.png")
        except Exception as se:
            log_print(f"Ошибка сохранения скриншота или HTML: {se}")
        return False

# Функция обработки страницы заведения
def process_establishment(url, index, total):
    driver = None
    try:
        driver = create_driver()
        log_print(f"Начало обработки заведения {index}/{total}: {url}")
        time.sleep(random.uniform(5, 10))

        log_print(f"Загрузка страницы {url}...")
        driver.get(url)
        log_print(f"Статус загрузки страницы: {driver.execute_script('return document.readyState')}")
        if not solve_captcha(driver):
            log_print(f"Не удалось решить капчу для {url}, продолжаем.")

        log_print(f"Ожидание заголовка страницы {url}...")
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.orgpage-header-view__header")))
        log_print(f"Страница {url} загружена.")

        previous_review_count = 0
        max_attempts = 100
        attempts = 0
        max_reviews = 200

        try:
            log_print("Поиск вкладки 'Отзывы'...")
            reviews_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='/reviews']"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", reviews_tab)
            driver.execute_script("arguments[0].click();", reviews_tab)
            time.sleep(random.uniform(1, 2))
            log_print("Переход на вкладку 'Отзывы' выполнен.")
        except Exception as e:
            log_print(f"Вкладка 'Отзывы' не найдена, продолжаем: {e}")

        while attempts < max_attempts and previous_review_count < max_reviews:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(1, 3))
            source = driver.page_source
            soup = BeautifulSoup(source, 'html.parser')
            reviews = soup.find_all('div', class_='business-review-view__body')
            current_review_count = len(reviews)
            if current_review_count > previous_review_count:
                previous_review_count = current_review_count
                attempts = 0
            else:
                attempts += 1
            log_print(f"Найдено отзывов: {current_review_count}, попытка {attempts}/{max_attempts}")
            if current_review_count >= max_reviews:
                log_print(f"Достигнут лимит {max_reviews} отзывов для {url}.")
                break
        log_print(f"Прокрутка страницы {url} завершена, найдено {current_review_count} отзывов.")

        data = {"href": url}
        name = soup.find('h1', class_='orgpage-header-view__header')
        data["name"] = name.text.strip() if name else "null"
        address = soup.find('div', class_='business-contacts-view__address-link')
        data["address"] = address.text.strip() if address else "null"
        phone = soup.find('div', class_='orgpage-phones-view__phone-number')
        data["phone"] = phone.text.strip() if phone else "null"
        
        rate_elements = soup.find_all('span', class_='business-summary-rating-badge-view__rating-text')
        rate = ''.join([elem.text.strip() for elem in rate_elements if elem.text.strip() != ',']) if rate_elements else "null"
        data["rate"] = rate.replace(',', '.') if rate != "null" else "null"
        
        rate_count = soup.find('span', class_='business-header-rating-view__text')
        data["rate_count"] = rate_count.text.strip().split()[0] if rate_count and rate_count.text.strip() else "null"
        site = soup.find('span', class_='business-urls-view__text')
        data["site"] = site.text.strip() if site else "null"
        average_bill = soup.find('span', class_='business-features-view__valued-value')
        data["average_bill"] = average_bill.text.strip() if average_bill else "null"

        establishment_id = save_establishment_to_db(data)

        reviews_list = []
        for review in soup.find_all('div', class_='business-review-view__body')[:max_reviews]:
            review_text = review.find('span', class_='business-review-view__body-text')
            author = review.find('span', class_='business-review-view__author')
            rating = review.find('div', class_='business-rating-badge-view__stars')
            rating_value = "N/A"
            if rating:
                stars = rating.find_all('span', class_='business-rating-badge-view__star _full')
                rating_value = str(len(stars))
            date = review.find('span', class_='business-review-view__date')
            review_data = {
                "text": review_text.text.strip() if review_text else "N/A",
                "author": author.text.strip() if author else "N/A",
                "rating": rating_value,
                "date": date.text.strip() if date else "N/A"
            }
            reviews_list.append(review_data)

        if reviews_list:
            save_reviews_to_db(establishment_id, reviews_list)

        log_print(f"Ссылка: {data['href']}\nНазвание: {data['name']}\nАдрес: {data['address']}\nТелефон: {data['phone']}\n"
                  f"Рейтинг: {data['rate']}\nКол-во отзывов: {data['rate_count']}\nСайт: {data['site']}\n"
                  f"Средний чек: {data['average_bill']}\nОтзывы: {len(reviews_list)} шт.\n---")
        log_print(f"Заведение {index}/{total} обработано.")

    except Exception as e:
        log_print(f"ERROR: Ошибка при обработке {url}: {str(e)}")
        with open("failed_urls.txt", "a") as f:
            f.write(f"{url} - {str(e)}\n")
    finally:
        if driver:
            driver.quit()
        time.sleep(random.uniform(2, 5))

# Основная функция
def main():
    # Проверка переменной окружения для переключения парсера
    if os.getenv("ENABLE_PARSER", "false").lower() != "true":
        log_print("Парсер выключен (ENABLE_PARSER=false). Завершение работы.")
        return

    # Получение параметров из переменных окружения с значениями по умолчанию
    title = os.getenv("TITLE", "кафе")
    base_url = os.getenv("BASE_URL", "https://yandex.ru/maps/213/moscow/search/кафе")
    count_of_units = 5000
    max_scroll_time = 1800  # 30 минут на поиск

    log_print(f"Запуск парсера для {title} по ссылке {base_url}")

    driver = None
    href_list = set()

    try:
        driver = create_driver()
        log_print("Драйвер Chrome инициализирован.")

        log_print(f"Тип URL: {type(base_url)}, Значение: {base_url}")
        log_print(f"Открытие страницы: {base_url}")

        for attempt in range(1, 4):
            try:
                log_print(f"Загрузка страницы {base_url} (попытка {attempt})...")
                driver.get(base_url)
                WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")
                log_print(f"Статус загрузки страницы: {driver.execute_script('return document.readyState')}")
                log_print(f"Страница {base_url} открыта (попытка {attempt}).")
                break
            except Exception as e:
                log_print(f"Ошибка загрузки {base_url} на попытке {attempt}: {str(e)}")
                if attempt == 3:
                    log_print(f"Не удалось загрузить {base_url} после 3 попыток.")
                    break
                time.sleep(random.uniform(5, 10))

        if not solve_captcha(driver):
            log_print(f"Не удалось решить капчу для {base_url}.")
            with open(f"error_page_{int(time.time())}.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            return

        time.sleep(random.uniform(10, 20))

        wait = WebDriverWait(driver, 300)
        try:
            log_print("Ожидание элементов...")
            timestamp = int(time.time())
            with open(f"pre_check_page_{timestamp}.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            try:
                driver.save_screenshot(f"pre_check_screenshot_{timestamp}.png")
                log_print(f"Скриншот сохранён: pre_check_screenshot_{timestamp}.png")
            except Exception as se:
                log_print(f"Ошибка сохранения скриншота: {se}")

            for _ in range(3):
                driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(random.uniform(2, 5))

            max_attempts = 3
            for attempt in range(max_attempts):
                possible_selectors = [
                    ".search-business-snippet-view__title",
                    ".search-snippet-view__link-overlay",
                    ".business-snippet__link"
                ]
                for selector in possible_selectors:
                    try:
                        elements = wait.until(
                            EC.visibility_of_all_elements_located((By.CSS_SELECTOR, selector))
                        )
                        if elements:
                            log_print(f"Найдено {len(elements)} элементов с селектором {selector}")
                            break
                    except:
                        continue
                else:
                    if attempt < max_attempts - 1:
                        log_print(f"Попытка {attempt + 1} неудачна, повторяем через 5 секунд...")
                        time.sleep(5)
                        continue
                    raise Exception("Ни один из селекторов не сработал после всех попыток")
        except Exception as e:
            log_print(f"Ошибка ожидания элементов: {str(e)}")
            log_print("Полный стек-трейс:")
            log_print(traceback.format_exc())
            with open(f"error_page_{int(time.time())}.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            try:
                driver.save_screenshot(f"error_screenshot_{int(time.time())}.png")
                log_print(f"Скриншот сохранён: error_screenshot_{int(time.time())}.png")
            except Exception as se:
                log_print(f"Ошибка сохранения скриншота: {se}")
            return

        n = 0
        start_time = time.time()
        while len(elements) < count_of_units and time.time() - start_time < max_scroll_time:
            if not solve_captcha(driver):
                log_print(f"Не удалось решить капчу во время прокрутки, продолжаем.")

            time.sleep(random.uniform(2, 5))
            elements_before = len(elements)

            try:
                log_print(f"Текущая высота страницы: {driver.execute_script('return document.body.scrollHeight')}")
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    last_element = elements[-1]
                    driver.execute_script("arguments[0].scrollIntoView(true);", last_element)
                    time.sleep(random.uniform(0.5, 1.0))
            except Exception as e:
                log_print(f"Ошибка при ожидании элементов: {e}")
                n += 1
                if n >= 10:
                    log_print(f"Скроллинг завершён из-за отсутствия новых элементов или ошибок.")
                    break
                continue

            elements_after = len(driver.find_elements(By.CSS_SELECTOR, selector))
            log_print(f"Найдено элементов: {elements_after} после прокрутки")

            if elements_before == elements_after:
                n += 1
                log_print(f"Нет новых элементов, итерация {n}")
                if n >= 10:
                    log_print(f"Скроллинг завершён из-за отсутствия новых элементов.")
                    break
            else:
                n = 0
                log_print("Найдены новые элементы.")

        for idx, title in enumerate(driver.find_elements(By.CSS_SELECTOR, ".search-business-snippet-view__title")):
            try:
                parent = title.find_element(By.XPATH, "./ancestor::div[contains(@class, 'search-business-snippet-view')]//a")
                href = parent.get_attribute('href')
                if href and href.strip():
                    href_list.add(href)
                    log_print(f"Ссылка {idx + 1} собрана: {href}")
                else:
                    log_print(f"Пропущен элемент без валидной ссылки (индекс {idx + 1})")
            except Exception as e:
                log_print(f"Не удалось собрать ссылку для элемента {idx + 1}: {e}")
                continue

        log_print(f"Итоговое количество уникальных ссылок: {len(href_list)}")

        href_list = list(href_list)[:count_of_units]
        with ThreadPoolExecutor(max_workers=5) as executor:
            for index, href in enumerate(href_list, 1):
                log_print(f"Запуск обработки ссылки {index}: {href}")
                executor.submit(process_establishment, href, index, len(href_list))

    except Exception as e:
        log_print(f"Ошибка: {e}")
        log_print("Полный стек-трейс:")
        log_print(traceback.format_exc())
        if driver:
            with open(f"error_page_main_{int(time.time())}.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
    finally:
        if driver:
            driver.quit()
        log_print("Завершение работы скрипта.")

if __name__ == "__main__":
    try:
        setup_database()
        main()
    except Exception as e:
        log_print(f"Ошибка: {e}")