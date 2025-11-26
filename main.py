import asyncio
import requests
from aiogram import Bot, Dispatcher
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.filters import Command
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import matplotlib.pyplot as plt
import io
import pandas as pd
import aiohttp
import json
import os
from lxml import html
import time

TOKEN = "8558938442:AAHf6KwcI6dzfoCAk7YtDwhjo04GeO4PH2k"
FINNHUB_KEY = "d4e98ghr01qgp2f7e5fgd4e98ghr01qgp2f7e5g0"

bot = Bot(TOKEN, timeout=60)
dp = Dispatcher()

user_states = {}
news_cache = {}
user_queries = {}
user_history = {}
user_portfolio = {}

if not os.path.exists('user_data'):
    os.makedirs('user_data')
if not os.path.exists('excel_reports'):
    os.makedirs('excel_reports')
if not os.path.exists('portfolio_data'):
    os.makedirs('portfolio_data')

# Создание кнопок
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Котировки"), KeyboardButton(text="Новости")],
        [KeyboardButton(text="Валютные пары"), KeyboardButton(text="Помощь")],
        [KeyboardButton(text="Отчет Excel"), KeyboardButton(text="Портфель")]
    ],
    resize_keyboard=True
)

portfolio_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Добавить в портфель"), KeyboardButton(text="Добавить количество")],
        [KeyboardButton(text="Мой портфель"), KeyboardButton(text="Отчет портфеля")],
        [KeyboardButton(text="Очистить портфель"), KeyboardButton(text="Назад в меню")]
    ],
    resize_keyboard=True
)

russian_securities_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Российские акции"), KeyboardButton(text="Российские облигации")],
        [KeyboardButton(text="Назад в меню")]
    ],
    resize_keyboard=True
)

securities_type_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Российские ценные бумаги"), KeyboardButton(text="Зарубежные ценные бумаги")],
        [KeyboardButton(text="Назад в меню")]
    ],
    resize_keyboard=True
)

news_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Экономические новости"), KeyboardButton(text="Политические новости")],
        [KeyboardButton(text="Новости по тикеру")],
        [KeyboardButton(text="Назад в меню")]
    ],
    resize_keyboard=True
)

currency_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="USD/RUB"), KeyboardButton(text="EUR/RUB")],
        [KeyboardButton(text="CNY/RUB"), KeyboardButton(text="Другая пара")],
        [KeyboardButton(text="Назад в меню")]
    ],
    resize_keyboard=True
)

back_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Назад в меню")]
    ],
    resize_keyboard=True
)

date_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Сегодня"), KeyboardButton(text="Вчера")],
        [KeyboardButton(text="Указать дату")]
    ],
    resize_keyboard=True
)

# Сохранение истории пользователей
def save_user_history(user_id, ticker, security_type, result):
    try:
        history_file = f'user_data/user_{user_id}_history.json'
        
        if user_id not in user_history:
            user_history[user_id] = []
        
        history_entry = {
            'timestamp': datetime.now().isoformat(),
            'ticker': ticker,
            'security_type': security_type,
            'result': result
        }
        
        user_history[user_id].append(history_entry)
        
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(user_history[user_id], f, ensure_ascii=False, indent=2)
            
    except Exception as e:
        print(f"Error saving user history: {e}")

def load_user_history(user_id):
    try:
        history_file = f'user_data/user_{user_id}_history.json'
        if os.path.exists(history_file):
            with open(history_file, 'r', encoding='utf-8') as f:
                user_history[user_id] = json.load(f)
        else:
            user_history[user_id] = []
    except Exception as e:
        print(f"Error loading user history: {e}")
        user_history[user_id] = []

def save_user_portfolio(user_id):
    try:
        portfolio_file = f'portfolio_data/portfolio_{user_id}.json'
        with open(portfolio_file, 'w', encoding='utf-8') as f:
            json.dump(user_portfolio.get(user_id, []), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error saving portfolio: {e}")

def load_user_portfolio(user_id):
    try:
        portfolio_file = f'portfolio_data/portfolio_{user_id}.json'
        if os.path.exists(portfolio_file):
            with open(portfolio_file, 'r', encoding='utf-8') as f:
                user_portfolio[user_id] = json.load(f)
        else:
            user_portfolio[user_id] = []
    except Exception as e:
        print(f"Error loading portfolio: {e}")
        user_portfolio[user_id] = []

def add_to_user_queries(user_id, quote_data):
    if user_id not in user_queries:
        user_queries[user_id] = []
    
    quote_data_with_time = quote_data.copy()
    quote_data_with_time['query_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    user_queries[user_id].append(quote_data_with_time)

# Отчеты в эксель
def create_excel_report(user_id):
    try:
        if user_id not in user_queries or not user_queries[user_id]:
            return None
        
        data = []
        for query in user_queries[user_id]:
            data.append({
                'Тикер': query['ticker'],
                'Компания': query['company_name'],
                'Тип': query['security_type'],
                'Цена': query['price'],
                'Изменение (%)': query['change_percent'],
                'Изменение (абс)': query['change_amount'],
                'Пред. закрытие': query['prev_close'],
                'Валюта': query['currency'],
                'Время запроса': query['query_time']
            })
        
        df = pd.DataFrame(data)
        
        filename = f'excel_reports/report_{user_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Котировки', index=False)
            
            worksheet = writer.sheets['Котировки']
            
            column_widths = {
                'A': 15, 'B': 30, 'C': 15, 'D': 12, 
                'E': 15, 'F': 15, 'G': 15, 'H': 10, 'I': 20
            }
            
            for col, width in column_widths.items():
                worksheet.column_dimensions[col].width = width
            
            worksheet['A1'] = f'Отчет по котировкам'
            worksheet['A2'] = f'Сгенерирован: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            worksheet['A3'] = f'Всего записей: {len(data)}'
        
        return filename
        
    except Exception as e:
        print(f"Error creating Excel report: {e}")
        return None

# Создание портфолио
def create_portfolio_report(user_id):
    try:
        if user_id not in user_portfolio or not user_portfolio[user_id]:
            return None
        
        data = []
        for item in user_portfolio[user_id]:
            data.append({
                'Тикер': item['ticker'],
                'Название': item.get('company_name', ''),
                'Тип': item.get('security_type', ''),
                'Цена покупки': item['buy_price'],
                'Количество': item['quantity'],
                'Общая стоимость': item['buy_price'] * item['quantity'],
                'Дата покупки': item['buy_date'],
                'Валюта': item.get('currency', 'RUB')
            })
        
        df = pd.DataFrame(data)
        
        filename = f'portfolio_data/portfolio_report_{user_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Портфель', index=False)
            
            worksheet = writer.sheets['Портфель']
            
            column_widths = {
                'A': 15, 'B': 30, 'C': 15, 'D': 15, 
                'E': 12, 'F': 15, 'G': 15, 'H': 10
            }
            
            for col, width in column_widths.items():
                worksheet.column_dimensions[col].width = width
            
            worksheet['A1'] = f'Отчет по портфелю'
            worksheet['A2'] = f'Сгенерирован: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            worksheet['A3'] = f'Всего позиций: {len(data)}'
            worksheet['A4'] = f'Общая стоимость: {df["Общая стоимость"].sum():.2f}'
        
        return filename
        
    except Exception as e:
        print(f"Error creating portfolio report: {e}")
        return None

# Курс валют
def get_currency_rate(currency_pair):
    try:
        if currency_pair in ["USD/RUB", "EUR/RUB", "CNY/RUB"]:
            url = "https://www.cbr-xml-daily.ru/daily_json.js"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                if currency_pair == "USD/RUB":
                    rate = data['Valute']['USD']['Value']
                    return rate, "RUB", "ЦБ РФ"
                elif currency_pair == "EUR/RUB":
                    rate = data['Valute']['EUR']['Value']
                    return rate, "RUB", "ЦБ РФ"
                elif currency_pair == "CNY/RUB":
                    rate = data['Valute']['CNY']['Value']
                    return rate, "RUB", "ЦБ РФ"
        
        else:
            if "/" in currency_pair:
                base_currency, target_currency = currency_pair.split("/")
                
                url = f"https://api.exchangerate-api.com/v4/latest/{base_currency}"
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'rates' in data and target_currency in data['rates']:
                        rate = data['rates'][target_currency]
                        return rate, target_currency, "ExchangeRate-API"
        
        return None, None, None
        
    except Exception as e:
        print(f"Currency rate error: {e}")
        return None, None, None

# Облигации
def get_bond_quote(ticker):
    try:
        moex_data = get_moex_bond_data(ticker)
        if moex_data and moex_data.get('price') is not None:
            return moex_data

        corpbonds_data = get_bond_from_corpbonds(ticker)
        if corpbonds_data and corpbonds_data.get('price') is not None:
            return corpbonds_data
        
        return get_bond_unavailable_data(ticker)
        
    except Exception as e:
        print(f"All bond sources failed for {ticker}: {e}")
        return get_bond_unavailable_data(ticker)

def get_moex_bond_data(ticker):
    try:
        if ticker.startswith('SU'):
            board = 'TQOB'
        elif ticker.startswith('RU'):
            board = 'TQCB'
        else:
            return None
        
        url = (f"https://iss.moex.com/iss/engines/stock/markets/bonds/boards/{board}"
               f"/securities/{ticker}.json?iss.meta=off")
        
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        
        if response.status_code != 200:
            return None
            
        data = response.json()
        
        securities_data = data.get('securities', {}).get('data', [])
        if not securities_data:
            return None
            
        security_info = securities_data[0]
        
        market_data = data.get('marketdata', {}).get('data', [])
        if not market_data:
            return None
            
        market_info = market_data[0]
        
        company_name = security_info[2] if len(security_info) > 2 else f"Облигация {ticker}"
        
        last_price = market_info[12] if len(market_info) > 12 else market_info[4]  
        if last_price is None:
            return None
        
        prev_close = security_info[11] if len(security_info) > 11 and security_info[11] else last_price
        
        if prev_close and prev_close > 0:
            change_percent = ((last_price - prev_close) / prev_close) * 100
            change_amount = last_price - prev_close
        else:
            change_percent = 0
            change_amount = 0
        
        if ticker.startswith('SU'):
            bond_type = "ОФЗ"
            company_name = f"ОФЗ {ticker}"
        else:
            bond_type = "Корпоративная"
            company_name = f"Корпоративная облигация {ticker}"
        
        face_value = 1000.0
        price_percent = (last_price / face_value) * 100
        
        chart_link = f"https://www.moex.com/ru/issue.aspx?code={ticker}"
        
        return {
            'company_name': company_name,
            'ticker': ticker,
            'price': last_price,
            'price_percent': price_percent,
            'face_value': face_value,
            'change_percent': change_percent,
            'change_amount': change_amount,
            'prev_close': prev_close,
            'security_type': 'облигация',
            'currency': 'RUB',
            'bond_type': bond_type,
            'board': board,
            'chart_link': chart_link,
            'source': 'MOEX'
        }
        
    except Exception as e:
        print(f"MOEX bond error for {ticker}: {e}")
        return None

def get_bond_from_corpbonds(ticker):
    try:
        url = f"https://corpbonds.ru/bond/{ticker}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            print(f"Corpbonds.ru returned status code: {response.status_code}")
            return None
        
        tree = html.fromstring(response.content)

        price_xpath = '//*[@id="root"]/main/section/header/main/article[4]/div/p[2]'

        price_elements = tree.xpath(price_xpath)
        
        if not price_elements:
            print(f"Price element not found for {ticker} using XPath: {price_xpath}")
            alternative_xpaths = [
                '//p[contains(text(), "Цена")]/following-sibling::p',
                '//div[contains(@class, "price")]',
                '//span[contains(@class, "price")]',
                '//*[contains(text(), "Текущая цена")]/following::p[1]'
            ]
            
            for alt_xpath in alternative_xpaths:
                price_elements = tree.xpath(alt_xpath)
                if price_elements:
                    print(f"Found price using alternative XPath: {alt_xpath}")
                    break
        
        if not price_elements:
            return None
        
        price_element = price_elements[0]
        price_text = price_element.text_content().strip()
        
        price = parse_price_from_text(price_text)
        
        if price is None:
            print(f"Could not parse price from text: '{price_text}'")
            return None

        company_name = get_company_name_from_corpbonds(tree, ticker)
        
        face_value = 1000.0
        price_percent = (price / face_value) * 100
        
        chart_link = f"https://www.moex.com/ru/issue.aspx?code={ticker}"
        
        return {
            'company_name': company_name,
            'ticker': ticker,
            'price': price,
            'price_percent': price_percent,
            'face_value': face_value,
            'change_percent': 0,  
            'change_amount': 0,   
            'prev_close': price, 
            'security_type': 'облигация',
            'currency': 'RUB',
            'bond_type': "ОФЗ" if ticker.startswith('SU') else "Корпоративная",
            'board': 'corpbonds.ru',
            'chart_link': chart_link,
            'source': ''
        }
        
    except Exception as e:
        print(f"Corpbonds.ru parsing error for {ticker}: {e}")
        return None

def parse_price_from_text(price_text):
    """Парсит цену из текста, полученного с сайта"""
    try:
        cleaned_text = re.sub(r'[^\d.,]', '', price_text.strip())
        
        cleaned_text = cleaned_text.replace(',', '.')
        
        if cleaned_text.count('.') > 1:
            parts = cleaned_text.split('.')
            cleaned_text = ''.join(parts[:-1]) + '.' + parts[-1]

        price = float(cleaned_text)
        
        if price < 100: 
            price = price * 10 
        
        return price
        
    except Exception as e:
        print(f"Error parsing price from text '{price_text}': {e}")
        return None

def get_company_name_from_corpbonds(tree, ticker):
    try:
        name_xpaths = [
            '//h1',
            '//title',
            '//*[contains(@class, "name")]',
            '//*[contains(@class, "title")]'
        ]
        
        for xpath in name_xpaths:
            elements = tree.xpath(xpath)
            if elements:
                name = elements[0].text_content().strip()
                if name and len(name) > 5: 
                    return name
        
        if ticker.startswith('SU'):
            return f"ОФЗ {ticker}"
        else:
            return f"Корпоративная облигация {ticker}"
            
    except Exception as e:
        print(f"Error getting company name: {e}")
        if ticker.startswith('SU'):
            return f"ОФЗ {ticker}"
        else:
            return f"Корпоративная облигация {ticker}"

def get_bond_unavailable_data(ticker):
    if ticker.startswith('SU'):
        bond_type = "ОФЗ"
        company_name = f"ОФЗ {ticker}"
    else:
        bond_type = "Корпоративная"
        company_name = f"Корпоративная облигация {ticker}"
    
    return {
        'company_name': company_name,
        'ticker': ticker,
        'price': None,
        'price_percent': None,
        'face_value': 1000.0,
        'change_percent': None,
        'change_amount': None,
        'prev_close': None,
        'security_type': 'облигация',
        'currency': 'RUB',
        'bond_type': bond_type,
        'board': 'Недоступно',
        'chart_link': f"https://www.moex.com/ru/issue.aspx?code={ticker}",
        'source': 'Данные временно недоступны',
        'error_message': 'Не удалось получить данные с MOEX и corpbonds.ru. Попробуйте позже.'
    }

# Российские акции
def moex_detailed_quote(ticker, security_type="акция"):
    try:
        if security_type == "акция":
            market, board = "shares", "TQBR"
            url = f"https://iss.moex.com/iss/engines/stock/markets/{market}/boards/{board}/securities/{ticker}.json"
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if (data.get("securities", {}).get("data") and 
                    data.get("marketdata", {}).get("data")):
                    
                    security_info = data["securities"]["data"][0]
                    market_data = data["marketdata"]["data"][0]
                    
                    company_name = security_info[2]
                    last_price = market_data[12] if market_data[12] is not None else market_data[4]
                    prev_close = security_info[11] if security_info[11] is not None else last_price
                    
                    if last_price is None or not isinstance(last_price, (int, float)):
                        return None
                        
                    if prev_close is None or not isinstance(prev_close, (int, float)):
                        prev_close = last_price
                    
                    if prev_close and last_price and prev_close > 0:
                        change_percent = ((last_price - prev_close) / prev_close) * 100
                        change_amount = last_price - prev_close
                    else:
                        change_percent = 0
                        change_amount = 0
                    
                    chart_link = f"https://www.moex.com/ru/issue.aspx?board=TQBR&code={ticker}"
                    
                    return {
                        'company_name': company_name,
                        'ticker': ticker,
                        'price': last_price,
                        'change_percent': change_percent,
                        'change_amount': change_amount,
                        'prev_close': prev_close,
                        'security_type': security_type,
                        'currency': 'RUB',
                        'board': board,
                        'chart_link': chart_link
                    }
        
        else:
            return get_bond_quote(ticker)
        
        return None
        
    except Exception as e:
        print(f"MOEX error: {e}")
        return None

# Зарубежные акции
def finnhub_detailed_quote(ticker):
    try:
        quote_url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_KEY}"
        quote_response = requests.get(quote_url, timeout=10)
        
        if quote_response.status_code != 200:
            return None
            
        quote_data = quote_response.json()
        
        company_url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={FINNHUB_KEY}"
        company_response = requests.get(company_url, timeout=10)
        
        company_name = ticker
        if company_response.status_code == 200:
            company_data = company_response.json()
            company_name = company_data.get('name', ticker)
        
        current_price = quote_data.get('c', 0)
        previous_close = quote_data.get('pc', current_price)
        
        if current_price is None or not isinstance(current_price, (int, float)) or current_price <= 0:
            return None
        
        if (previous_close and current_price and previous_close > 0 and 
            isinstance(previous_close, (int, float)) and isinstance(current_price, (int, float))):
            change_percent = ((current_price - previous_close) / previous_close) * 100
            change_amount = current_price - previous_close
        else:
            change_percent = 0
            change_amount = 0
        
        chart_link = f"https://www.tradingview.com/symbols/{ticker}/"
        
        return {
            'company_name': company_name,
            'ticker': ticker,
            'price': current_price,
            'change_percent': change_percent,
            'change_amount': change_amount,
            'prev_close': previous_close,
            'security_type': 'акция',
            'currency': 'USD',
            'board': 'NASDAQ',
            'chart_link': chart_link
        }
        
    except Exception as e:
        print(f"Finnhub error: {e}")
        return None

# Создание графиков
def create_simple_chart(quote_data, security_type):
    try:
        plt.figure(figsize=(8, 4))
        
        times = ['Пред. закрытие', 'Текущая цена']
        prices = [quote_data['prev_close'], quote_data['price']]
        
        color = 'red' if quote_data['change_percent'] < 0 else 'green'
        plt.plot(times, prices, marker='o', linewidth=2, markersize=6, color=color)
        
        for i, (time, price) in enumerate(zip(times, prices)):
            plt.annotate(f'{price:.2f}', (time, price), textcoords="offset points", 
                        xytext=(0,8), ha='center', fontsize=8)
        
        plt.title(f'{quote_data["ticker"]} ({security_type})', fontsize=10)
        plt.ylabel('Цена')
        plt.grid(True, alpha=0.3)
        
        if quote_data['change_percent'] < 0:
            plt.gca().set_facecolor('#fff5f5')
        else:
            plt.gca().set_facecolor('#f5fff5')
        
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=60, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        return buf
        
    except Exception as e:
        print(f"Chart error: {e}")
        return None

# Получение новостей по тикеру
def get_ticker_news_investing(ticker):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        if any(char in ticker for char in 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ'):
            search_ticker = f"{ticker}.ME"
        else:
            search_ticker = ticker
        
        search_url = f"https://ru.investing.com/search/?q={search_ticker}"
        response = requests.get(search_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return []
            
        soup = BeautifulSoup(response.content, 'html.parser')
        news_items = []
        
        news_elements = soup.find_all('div', class_=['articleItem'])
        
        for element in news_elements[:5]:
            try:
                title_elem = element.find('a', class_=['title'])
                if not title_elem:
                    continue
                    
                title = title_elem.get_text().strip()
                link = title_elem.get('href', '')
                
                if link and not link.startswith('http'):
                    link = f"https://ru.investing.com{link}"
                
                date_elem = element.find('span', class_=['date'])
                date_text = date_elem.get_text().strip() if date_elem else "Сегодня"
                
                if 'месяц' in date_text.lower():
                    continue
                
                news_key = f"investing_{hash(title + link)}"
                
                one_month_ago = datetime.now() - timedelta(days=30)
                if news_key not in news_cache or news_cache[news_key] >= one_month_ago:
                    news_cache[news_key] = datetime.now()
                    
                    news_items.append({
                        'title': title[:150],
                        'link': link,
                        'time': date_text,
                        'source': 'Investing.com',
                        'key': news_key
                    })
                    
            except Exception:
                continue
        
        return news_items
        
    except Exception as e:
        print(f"Investing.com news error: {e}")
        return []

def get_ticker_news_marketwatch(ticker):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        search_url = f"https://www.marketwatch.com/investing/stock/{ticker}"
        response = requests.get(search_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return []
            
        soup = BeautifulSoup(response.content, 'html.parser')
        news_items = []
        
        news_elements = soup.find_all('div', class_=['element--article'])
        
        for element in news_elements[:5]:
            try:
                title_elem = element.find('h3', class_=['article__headline'])
                if not title_elem:
                    continue
                    
                title = title_elem.get_text().strip()
                link_elem = title_elem.find('a')
                if not link_elem:
                    continue
                    
                link = link_elem.get('href', '')
                if link and not link.startswith('http'):
                    link = f"https://www.marketwatch.com{link}"
                
                date_elem = element.find('span', class_=['article__timestamp'])
                date_text = date_elem.get_text().strip() if date_elem else "Сегодня"
                
                news_key = f"marketwatch_{hash(title + link)}"
                
                one_month_ago = datetime.now() - timedelta(days=30)
                if news_key not in news_cache or news_cache[news_key] >= one_month_ago:
                    news_cache[news_key] = datetime.now()
                    
                    news_items.append({
                        'title': title[:150],
                        'link': link,
                        'time': date_text,
                        'source': 'MarketWatch',
                        'key': news_key
                    })
                    
            except Exception:
                continue
        
        return news_items
        
    except Exception as e:
        print(f"MarketWatch news error: {e}")
        return []
    
# Получение новостей из Риа новости
def get_recent_news_ria(category="economic"):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        ria_urls = {
            "economic": "https://ria.ru/economy/",
            "political": "https://ria.ru/politics/"
        }
        
        url = ria_urls.get(category, ria_urls["economic"])
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        news_items = []
        
        articles = soup.find_all('div', class_=['list-item', 'cell-list__item'])[:8]
        
        for article in articles:
            try:
                title_elem = article.find(['a', 'div'], class_=['list-item__title', 'cell-list__item-title'])
                link_elem = article.find('a', href=True)
                
                if title_elem and link_elem:
                    title = title_elem.get_text().strip()[:100]
                    link = link_elem['href']
                    
                    if link.startswith('/'):
                        link = f"https://ria.ru{link}"
                    
                    time_elem = article.find('div', class_=['list-item__date', 'cell-list__item-date'])
                    time_text = time_elem.get_text().strip() if time_elem else ""
                    
                    news_key = f"ria_{hash(title)}"
                    
                    one_week_ago = datetime.now() - timedelta(days=7)
                    if news_key not in news_cache or news_cache[news_key] >= one_week_ago:
                        news_cache[news_key] = datetime.now()
                        
                        news_items.append({
                            'title': title,
                            'link': link,
                            'time': time_text,
                            'source': 'РИА Новости',
                            'key': news_key
                        })
            except Exception:
                continue
                
        return news_items
        
    except Exception as e:
        print(f"RIA news error: {e}")
        return []

# Получение новстей из Тасс
def get_recent_news_tass(category="economic"):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        tass_urls = {
            "economic": "https://tass.ru/ekonomika",
            "political": "https://tass.ru/politika"
        }
        
        url = tass_urls.get(category, tass_urls["economic"])
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        news_items = []
        
        articles = soup.find_all('div', class_=['news-card', 'card-news'])[:8]
        
        for article in articles:
            try:
                title_elem = article.find(['a', 'h3'], class_=['news-card__title', 'card-news__title'])
                link_elem = article.find('a', href=True)
                
                if title_elem and link_elem:
                    title = title_elem.get_text().strip()[:100]
                    link = link_elem['href']
                    
                    if link.startswith('/'):
                        link = f"https://tass.ru{link}"
                    
                    news_key = f"tass_{hash(title)}"
                    
                    one_week_ago = datetime.now() - timedelta(days=7)
                    if news_key not in news_cache or news_cache[news_key] >= one_week_ago:
                        news_cache[news_key] = datetime.now()
                        
                        news_items.append({
                            'title': title,
                            'link': link,
                            'time': '',
                            'source': 'ТАСС',
                            'key': news_key
                        })
            except Exception:
                continue
                
        return news_items
        
    except Exception as e:
        print(f"TASS news error: {e}")
        return []

# Экономические новости
def get_economic_news():
    try:
        sources = [
            get_recent_news_ria("economic"),
            get_recent_news_tass("economic")
        ]
        
        all_news = []
        for news_list in sources:
            if news_list:
                all_news.extend(news_list)
        
        seen_keys = set()
        unique_news = []
        
        for news in all_news:
            if news['key'] not in seen_keys:
                seen_keys.add(news['key'])
                unique_news.append(news)
        
        return unique_news[:5]
        
    except Exception as e:
        print(f"Economic news error: {e}")
        return []

# Политические новости
def get_political_news():
    try:
        sources = [
            get_recent_news_ria("political"),
            get_recent_news_tass("political")
        ]
        
        all_news = []
        for news_list in sources:
            if news_list:
                all_news.extend(news_list)
        
        seen_keys = set()
        unique_news = []
        
        for news in all_news:
            if news['key'] not in seen_keys:
                seen_keys.add(news['key'])
                unique_news.append(news)
        
        return unique_news[:5]
        
    except Exception as e:
        print(f"Political news error: {e}")
        return []

def get_news_by_ticker(ticker):
    try:
        news_sources = [
            get_ticker_news_investing(ticker),
            get_ticker_news_marketwatch(ticker)
        ]
        
        all_news = []
        for news_list in news_sources:
            if news_list:
                all_news.extend(news_list)
        
        if all_news:
            seen_keys = set()
            unique_news = []
            
            for news in all_news:
                if news['key'] not in seen_keys:
                    seen_keys.add(news['key'])
                    unique_news.append(news)
            
            return unique_news[:5]
        
        return [{
            'title': f'Актуальные новости по {ticker} в данный момент отсутствуют',
            'link': f'https://ru.investing.com/search/?q={ticker}',
            'time': 'Сегодня',
            'source': 'Investing.com',
            'key': f'info_{ticker}_{datetime.now().timestamp()}'
        }]
        
    except Exception as e:
        print(f"Ticker news error: {e}")
        return [{
            'title': f'Новости по {ticker} будут доступны в ближайшее время',
            'link': f'https://ru.investing.com/search/?q={ticker}',
            'time': 'Сегодня',
            'source': 'Investing.com',
            'key': f'fallback_{ticker}_{datetime.now().timestamp()}'
        }]
    
# Основные сообщения
@dp.message(Command("start"))
async def start(message: Message):
    user_id = message.from_user.id
    user_states[user_id] = {"mode": None, "awaiting_ticker": False}
    load_user_history(user_id)
    load_user_portfolio(user_id)
    
    welcome_text = """
Добро пожаловать в бот для отслеживания котировок и новостей!

Основные функции:
- Котировки российских и зарубежных ценных бумаг
- Актуальные экономические и политические новости
- Новости по конкретным компаниям
- Валютные пары
- Excel отчеты по запрошенным котировкам
- Управление инвестиционным портфелем

Для начала работы выберите раздел в меню ниже.
"""
    await message.answer(welcome_text, reply_markup=main_kb)

@dp.message(Command("help"))
async def help_command(message: Message):
    help_text = """
Как пользоваться ботом:

КОТИРОВКИ:
1. Нажмите "Котировки"
2. Выберите тип ценных бумаг
3. Для российских бумаг выберите акции или облигации
4. Введите тикер

НОВОСТИ:
1. Нажмите "Новости" 
2. Выберите тип новостей
- Общие новости: за последнюю неделю
- Новости по тикеру: по конкретной компании

ВАЛЮТНЫЕ ПАРЫ:
1. Нажмите "Валютные пары"
2. Выберите нужную валютную пару или введите свою пару (например: EUR/USD)

ОТЧЕТ EXCEL:
- Все запрошенные котировки сохраняются
- Нажмите "Отчет Excel" для скачивания отчета

ПОРТФЕЛЬ:
- Добавить в портфель - новая позиция
- Добавить количество - добавить акции к существующей позиции
- Мой портфель - просмотр портфеля
- Отчет портфеля - скачать Excel отчет
- Очистить портфель - удалить все позиции

Примеры тикеров:
Российские акции: SBER, GAZP, VTBR, YNDX
Российские облигации: SU26230RMFS1, RU000A10DJV9
Зарубежные: AAPL, TSLA, GOOGL
"""
    await message.answer(help_text)

@dp.message(lambda m: m.text == "Котировки")
async def ask_securities_type(message: Message):
    user_id = message.from_user.id
    user_states[user_id] = {"mode": "securities", "awaiting_ticker": False}
    await message.answer("Выберите тип ценных бумаг:", reply_markup=securities_type_kb)

@dp.message(lambda m: m.text == "Новости")
async def show_news_menu(message: Message):
    user_id = message.from_user.id
    user_states[user_id] = {"mode": "news", "awaiting_ticker": False}
    
    news_text = """
Выберите тип новостей:

Экономические новости - бизнес и финансы
Политические новости - государственные вопросы
Новости по тикеру - по конкретной компании
"""
    await message.answer(news_text, reply_markup=news_kb)

@dp.message(lambda m: m.text == "Валютные пары")
async def show_currency_menu(message: Message):
    user_id = message.from_user.id
    user_states[user_id] = {"mode": "currency", "awaiting_ticker": False}
    
    currency_text = """
Выберите валютную пару:

USD/RUB - Доллар к рублю
EUR/RUB - Евро к рублю
CNY/RUB - Юань к рублю
Другая пара - Введите свою валютную пару

Примеры: EUR/USD, GBP/USD, USD/JPY
"""
    await message.answer(currency_text, reply_markup=currency_kb)

@dp.message(lambda m: m.text == "Отчет Excel")
async def generate_excel_report(message: Message):
    user_id = message.from_user.id
    
    if user_id not in user_queries or not user_queries[user_id]:
        await message.answer("У вас пока нет запрошенных котировок для отчета. Сначала запросите котировки через меню 'Котировки'.")
        return
    
    await message.answer("Создаю Excel отчет...")
    
    filename = create_excel_report(user_id)
    
    if filename:
        try:
            with open(filename, 'rb') as file:
                excel_file = BufferedInputFile(file.read(), filename=f"report_{user_id}.xlsx")
                await message.answer_document(
                    excel_file,
                    caption=f"Отчет по котировкам\nСгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M')}\nЗаписей: {len(user_queries[user_id])}\nФайл содержит все запрошенные вами котировки"
                )
            
            user_queries[user_id] = []
            
        except Exception as e:
            print(f"Error sending Excel file: {e}")
            await message.answer("Ошибка при отправке файла. Попробуйте позже.")
    else:
        await message.answer("Ошибка при создания отчета. Убедитесь, что установлена библиотека openpyxl: pip install openpyxl")

@dp.message(lambda m: m.text == "Портфель")
async def show_portfolio_menu(message: Message):
    user_id = message.from_user.id
    user_states[user_id] = {"mode": "portfolio", "awaiting_ticker": False}
    
    portfolio_text = """
Управление инвестиционным портфелем:

Добавить в портфель - добавить новую позицию
Добавить количество - добавить акции к существующей позиции
Мой портфель - просмотреть текущий портфель
Отчет портфеля - скачать Excel отчет по портфелю
Очистить портфель - удалить все записи из портфеля
"""
    await message.answer(portfolio_text, reply_markup=portfolio_kb)

@dp.message(lambda m: m.text == "Добавить в портфель")
async def add_to_portfolio_start(message: Message):
    user_id = message.from_user.id
    user_states[user_id] = {
        "mode": "portfolio",
        "action": "add_ticker",
        "awaiting_ticker": True
    }
    await message.answer("Введите тикер ценной бумаги:", reply_markup=back_kb)

@dp.message(lambda m: m.text == "Добавить количество")
async def add_quantity_start(message: Message):
    user_id = message.from_user.id
    
    if user_id not in user_portfolio or not user_portfolio[user_id]:
        await message.answer("Ваш портфель пуст. Сначала добавьте позиции через меню 'Добавить в портфель'.")
        return
    
    user_states[user_id] = {
        "mode": "portfolio",
        "action": "add_quantity_ticker",
        "awaiting_ticker": True
    }
    
    portfolio_text = "Выберите тикер из вашего портфеля:\n\n"
    for i, item in enumerate(user_portfolio[user_id], 1):
        portfolio_text += f"{i}. {item['ticker']} - {item.get('company_name', '')}\n"
    
    portfolio_text += "\nВведите тикер:"
    await message.answer(portfolio_text, reply_markup=back_kb)

@dp.message(lambda m: m.text == "Мой портфель")
async def show_my_portfolio(message: Message):
    user_id = message.from_user.id
    
    if user_id not in user_portfolio or not user_portfolio[user_id]:
        await message.answer("Ваш портфель пуст. Добавьте ценные бумаги через меню 'Добавить в портфель'.")
        return
    
    portfolio_text = "Ваш инвестиционный портфель:\n\n"
    total_value = 0
    
    for i, item in enumerate(user_portfolio[user_id], 1):
        item_value = item['buy_price'] * item['quantity']
        total_value += item_value
        
        portfolio_text += f"{i}. {item['ticker']} - {item.get('company_name', '')}\n"
        portfolio_text += f"   Цена покупки: {item['buy_price']:.2f} {item.get('currency', 'RUB')}\n"
        portfolio_text += f"   Количество: {item['quantity']}\n"
        portfolio_text += f"   Общая стоимость: {item_value:.2f} {item.get('currency', 'RUB')}\n"
        portfolio_text += f"   Дата покупки: {item['buy_date']}\n\n"
    
    portfolio_text += f"Общая стоимость портфеля: {total_value:.2f} RUB"
    
    await message.answer(portfolio_text[:4096])

@dp.message(lambda m: m.text == "Отчет портфеля")
async def generate_portfolio_report(message: Message):
    user_id = message.from_user.id
    
    if user_id not in user_portfolio or not user_portfolio[user_id]:
        await message.answer("Ваш портфель пуст. Добавьте ценные бумаги через меню 'Добавить в портфель'.")
        return
    
    await message.answer("Создаю отчет по портфелю...")
    
    filename = create_portfolio_report(user_id)
    
    if filename:
        try:
            with open(filename, 'rb') as file:
                excel_file = BufferedInputFile(file.read(), filename=f"portfolio_{user_id}.xlsx")
                await message.answer_document(
                    excel_file,
                    caption=f"Отчет по портфелю\nСгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M')}\nПозиций: {len(user_portfolio[user_id])}"
                )
            
        except Exception as e:
            print(f"Error sending portfolio file: {e}")
            await message.answer("Ошибка при отправке файла. Попробуйте позже.")
    else:
        await message.answer("Ошибка при создания отчета по портфелю.")

@dp.message(lambda m: m.text == "Очистить портфель")
async def clear_portfolio(message: Message):
    user_id = message.from_user.id
    
    if user_id in user_portfolio and user_portfolio[user_id]:
        user_portfolio[user_id] = []
        save_user_portfolio(user_id)
        await message.answer("Портфель успешно очищен.")
    else:
        await message.answer("Портфель уже пуст.")

@dp.message(lambda m: m.text == "Помощь")
async def show_help(message: Message):
    await help_command(message)

@dp.message(lambda m: m.text == "Назад в меню")
async def back_to_main(message: Message):
    user_id = message.from_user.id
    user_states[user_id] = {"mode": None, "awaiting_ticker": False}
    await message.answer("Главное меню", reply_markup=main_kb)

@dp.message(lambda m: m.text in ["Российские ценные бумаги", "Зарубежные ценные бумаги"])
async def handle_securities_type(message: Message):
    user_id = message.from_user.id
    
    if message.text == "Российские ценные бумаги":
        user_states[user_id] = {"mode": "russian_securities", "awaiting_ticker": False}
        instruction = "Выберите тип российских ценных бумаг:"
        await message.answer(instruction, reply_markup=russian_securities_kb)
    else:
        user_states[user_id] = {
            "mode": "securities", 
            "securities_type": "finnhub",
            "awaiting_ticker": True
        }
        instruction = "Введите тикер зарубежной ценной бумаги:"
        await message.answer(instruction, reply_markup=back_kb)

@dp.message(lambda m: m.text in ["Российские акции", "Российские облигации"])
async def handle_russian_securities(message: Message):
    user_id = message.from_user.id
    
    if message.text == "Российские акции":
        user_states[user_id] = {
            "mode": "securities", 
            "securities_type": "moex",
            "security_subtype": "акция",
            "awaiting_ticker": True
        }
        instruction = "Введите тикер российской акции:"
    else:
        user_states[user_id] = {
            "mode": "securities", 
            "securities_type": "moex",
            "security_subtype": "облигация",
            "awaiting_ticker": True
        }
        instruction = "Введите тикер российской облигации:"
    
    await message.answer(instruction, reply_markup=back_kb)

@dp.message(lambda m: m.text in ["Экономические новости", "Политические новости", "Новости по тикеру"])
async def handle_news_type(message: Message):
    user_id = message.from_user.id
    
    if message.text == "Новости по тикеру":
        user_states[user_id] = {
            "mode": "news",
            "news_type": "ticker",
            "awaiting_ticker": True
        }
        await message.answer("Введите тикер компании для поиска новостей:", reply_markup=back_kb)
    else:
        news_type = "economic" if message.text == "Экономические новости" else "political"
        user_states[user_id] = {"mode": "news", "awaiting_ticker": False}
        
        await message.answer("Загружаю новые публикации...")
        
        if news_type == "economic":
            news_items = get_economic_news()
        else:
            news_items = get_political_news()
        
        if not news_items:
            await message.answer("Новые публикации не найдены.")
            return
        
        news_text = f"{message.text}:\n\n"
        
        for i, item in enumerate(news_items, 1):
            news_text += f"{i}. {item['title']}\n"
            if item.get('time'):
                news_text += f"   Время: {item['time']}\n"
            news_text += f"   Источник: {item['source']}\n"
            news_text += f"   Ссылка: {item['link']}\n\n"
        
        await message.answer(news_text[:4096])

@dp.message(lambda m: m.text in ["USD/RUB", "EUR/RUB", "CNY/RUB", "Другая пара"])
async def handle_currency_pair(message: Message):
    user_id = message.from_user.id
    
    if message.text == "Другая пара":
        user_states[user_id] = {
            "mode": "currency",
            "awaiting_ticker": True
        }
        await message.answer("Введите валютную пару (например: EUR/USD, GBP/USD, USD/JPY):", reply_markup=back_kb)
    else:
        user_states[user_id] = {"mode": "currency", "awaiting_ticker": False}
        
        await message.answer(f"Получаю курс {message.text}...")
        
        rate, target_currency, source = get_currency_rate(message.text)
        
        if rate is not None:
            result_text = f"""
Валютная пара: {message.text}
Курс: 1 {message.text.split('/')[0]} = {rate:.4f} {message.text.split('/')[1]}
Источник: {source}

Обновлено: {datetime.now().strftime('%H:%M %d.%m.%Y')}
"""
            await message.answer(result_text)
        else:
            await message.answer(f"Не удалось получить курс {message.text}. Попробуйте позже.")

@dp.message()
async def handle_message(message: Message):
    user_id = message.from_user.id
    user_state = user_states.get(user_id, {"mode": None, "awaiting_ticker": False})
    
    if user_state.get("awaiting_ticker"):
        ticker = message.text.upper().strip()
        
        if not ticker:
            await message.answer("Введите тикер:")
            return
        
        if user_state.get("mode") == "securities":
            securities_type = user_state.get("securities_type")
            security_subtype = user_state.get("security_subtype", "акция")
            
            await message.answer(f"Получаю котировки для {ticker}...")
            
            if securities_type == "moex":
                quote_data = moex_detailed_quote(ticker, security_subtype)
            else:
                quote_data = finnhub_detailed_quote(ticker)
            
            if not quote_data:
                await message.answer(f"Не удалось получить данные для тикера {ticker}. Проверьте правильность тикера и попробуйте снова.")
                return
            
            add_to_user_queries(user_id, quote_data)
            save_user_history(user_id, ticker, quote_data['security_type'], "success")
            
            change_indicator = "▼" if quote_data.get('change_percent', 0) < 0 else "▲"
            
            if quote_data['security_type'] == 'облигация':
                if quote_data.get('error_message'):
                    result_text = f"""
{quote_data['company_name']} ({ticker})

{quote_data['error_message']}

Тип: {quote_data['security_type']} ({quote_data.get('bond_type', '')})
"""
                else:
                    source_info = f" ({quote_data.get('source', '')})" if quote_data.get('source') else ""
                    result_text = f"""
{change_indicator} {quote_data['company_name']} ({ticker}){source_info}

Цена: {quote_data['price']:.2f} {quote_data['currency']}
Цена в % от номинала: {quote_data.get('price_percent', 100):.2f}%
Номинал: {quote_data.get('face_value', 1000):.2f} {quote_data['currency']}
"""
                    
                    
                    if quote_data.get('change_percent') is not None:
                        result_text += f"Изменение: {quote_data['change_amount']:+.2f} ({quote_data['change_percent']:+.2f}%)\n"
                        result_text += f"Пред. закрытие: {quote_data['prev_close']:.2f}\n"
                    
                    result_text += f"""
Тип: {quote_data['security_type']} ({quote_data.get('bond_type', '')})
Биржа: {quote_data.get('board', 'Не указано')}

Подробный график: {quote_data.get('chart_link', '')}

Обновлено: {datetime.now().strftime('%H:%M %d.%m.%Y')}
"""
            else:
                result_text = f"""
{change_indicator} {quote_data['company_name']} ({ticker})

Текущая цена: {quote_data['price']:.2f} {quote_data['currency']}
Изменение: {quote_data['change_amount']:+.2f} ({quote_data['change_percent']:+.2f}%)
Пред. закрытие: {quote_data['prev_close']:.2f}
Тип: {quote_data['security_type']}
Биржа: {quote_data.get('board', 'Не указано')}

Подробный график: {quote_data.get('chart_link', '')}

Обновлено: {datetime.now().strftime('%H:%M %d.%m.%Y')}
"""
            await message.answer(result_text)
            
            # Создание графика только если есть данные о предыдущем закрытии
            if quote_data.get('prev_close') is not None and quote_data.get('price') is not None:
                chart = create_simple_chart(quote_data, quote_data['security_type'])
                if chart:
                    await message.answer_photo(BufferedInputFile(chart.getvalue(), filename="chart.png"))
            
            user_states[user_id] = {"mode": None, "awaiting_ticker": False}
        
        elif user_state.get("mode") == "news" and user_state.get("news_type") == "ticker":
            await message.answer(f"Ищу новости по тикеру {ticker}...")
            
            news_items = get_news_by_ticker(ticker)
            
            if not news_items:
                await message.answer(f"Новости по тикеру {ticker} не найдены.")
                return
            
            news_text = f"Новости по {ticker}:\n\n"
            
            for i, item in enumerate(news_items, 1):
                news_text += f"{i}. {item['title']}\n"
                if item.get('time'):
                    news_text += f"   Время: {item['time']}\n"
                news_text += f"   Источник: {item['source']}\n"
                news_text += f"   Ссылка: {item['link']}\n\n"
            
            await message.answer(news_text[:4096])
            user_states[user_id] = {"mode": None, "awaiting_ticker": False}
        
        elif user_state.get("mode") == "currency":
            await message.answer(f"Получаю курс {ticker}...")
            
            rate, target_currency, source = get_currency_rate(ticker)
            
            if rate is not None:
                base_currency = ticker.split('/')[0]
                result_text = f"""
Валютная пара: {ticker}
Курс: 1 {base_currency} = {rate:.4f} {target_currency}
Источник: {source}

Обновлено: {datetime.now().strftime('%H:%M %d.%m.%Y')}
"""
                await message.answer(result_text)
            else:
                await message.answer(f"Не удалось получить курс {ticker}. Проверьте правильность формата (например: EUR/USD) и попробуйте снова.")
            
            user_states[user_id] = {"mode": None, "awaiting_ticker": False}
        
        elif user_state.get("mode") == "portfolio" and user_state.get("action") == "add_ticker":
            user_states[user_id] = {
                "mode": "portfolio",
                "action": "add_price",
                "ticker": ticker,
                "awaiting_ticker": False
            }
            await message.answer(f"Тикер {ticker} сохранен. Введите цену покупки:")
        
        elif user_state.get("mode") == "portfolio" and user_state.get("action") == "add_quantity_ticker":
            found_item = None
            for item in user_portfolio[user_id]:
                if item['ticker'] == ticker:
                    found_item = item
                    break
            
            if not found_item:
                await message.answer(f"Тикер {ticker} не найден в вашем портфеле. Введите существующий тикер:")
                return
            
            user_states[user_id] = {
                "mode": "portfolio",
                "action": "add_quantity_amount",
                "ticker": ticker,
                "current_quantity": found_item['quantity'],
                "awaiting_ticker": False
            }
            await message.answer(f"Текущее количество {ticker}: {found_item['quantity']} акций. Введите количество для добавления:")
    
    elif user_state.get("mode") == "portfolio" and user_state.get("action") == "add_price":
        try:
            buy_price = float(message.text.replace(',', '.'))
            if buy_price <= 0:
                await message.answer("Цена должна быть положительным числом. Введите цену покупки:")
                return
            
            user_states[user_id] = {
                "mode": "portfolio",
                "action": "add_quantity",
                "ticker": user_state["ticker"],
                "buy_price": buy_price,
                "awaiting_ticker": False
            }
            await message.answer("Введите количество ценных бумаг:")
        except ValueError:
            await message.answer("Неверный формат цены. Введите число (например: 150.50):")
    
    elif user_state.get("mode") == "portfolio" and user_state.get("action") == "add_quantity":
        try:
            quantity = int(message.text)
            if quantity <= 0:
                await message.answer("Количество должно быть положительным числом. Введите количество:")
                return
            
            user_states[user_id] = {
                "mode": "portfolio",
                "action": "add_date",
                "ticker": user_state["ticker"],
                "buy_price": user_state["buy_price"],
                "quantity": quantity,
                "awaiting_ticker": False
            }
            await message.answer("Выберите дату покупки:", reply_markup=date_kb)
        except ValueError:
            await message.answer("Неверный формат количества. Введите целое число:")
    
    elif user_state.get("mode") == "portfolio" and user_state.get("action") == "add_quantity_amount":
        try:
            add_quantity = int(message.text)
            if add_quantity <= 0:
                await message.answer("Количество должно быть положительным числом. Введите количество:")
                return
            
            for item in user_portfolio[user_id]:
                if item['ticker'] == user_state["ticker"]:
                    item['quantity'] += add_quantity
                    break
            
            save_user_portfolio(user_id)
            
            await message.answer(
                f"Количество акций {user_state['ticker']} увеличено на {add_quantity}\n"
                f"Новое общее количество: {user_state['current_quantity'] + add_quantity}",
                reply_markup=portfolio_kb
            )
            
            user_states[user_id] = {"mode": "portfolio", "awaiting_ticker": False}
            
        except ValueError:
            await message.answer("Неверный формат количества. Введите целое число:")
    
    elif user_state.get("mode") == "portfolio" and user_state.get("action") == "add_date":
        if message.text == "Сегодня":
            buy_date = datetime.now().strftime('%Y-%m-%d')
            await complete_portfolio_addition(user_id, user_state, buy_date, message)
        elif message.text == "Вчера":
            buy_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            await complete_portfolio_addition(user_id, user_state, buy_date, message)
        elif message.text == "Указать дату":
            user_states[user_id] = {
                "mode": "portfolio",
                "action": "add_custom_date",
                "ticker": user_state["ticker"],
                "buy_price": user_state["buy_price"],
                "quantity": user_state["quantity"],
                "awaiting_ticker": True
            }
            await message.answer("Введите дату в формате ДД.ММ.ГГГГ (например: 20.11.2024):", reply_markup=back_kb)
        else:
            await message.answer("Выберите дату из предложенных вариантов:", reply_markup=date_kb)
    
    elif user_state.get("mode") == "portfolio" and user_state.get("action") == "add_custom_date":
        try:
            buy_date = datetime.strptime(message.text, '%d.%m.%Y').strftime('%Y-%m-%d')
            await complete_portfolio_addition(user_id, user_state, buy_date, message)
        except ValueError:
            try:
                buy_date = datetime.strptime(message.text, '%Y-%m-%d').strftime('%Y-%m-%d')
                await complete_portfolio_addition(user_id, user_state, buy_date, message)
            except ValueError:
                await message.answer("Неверный формат даты. Введите дату в формате ДД.ММ.ГГГГ (например: 20.11.2024):")
    
    else:
        await message.answer("Выберите действие из меню.", reply_markup=main_kb)

async def complete_portfolio_addition(user_id, user_state, buy_date, message):
    quote_data = moex_detailed_quote(user_state["ticker"], "акция")
    if not quote_data:
        quote_data = finnhub_detailed_quote(user_state["ticker"])
    
    company_name = quote_data['company_name'] if quote_data else user_state["ticker"]
    currency = quote_data['currency'] if quote_data else "RUB"
    security_type = quote_data['security_type'] if quote_data else "акция"
    
    if user_id not in user_portfolio:
        user_portfolio[user_id] = []
    
    portfolio_item = {
        'ticker': user_state["ticker"],
        'company_name': company_name,
        'security_type': security_type,
        'buy_price': user_state["buy_price"],
        'quantity': user_state["quantity"],
        'buy_date': buy_date,
        'currency': currency
    }
    
    user_portfolio[user_id].append(portfolio_item)
    save_user_portfolio(user_id)
    
    total_cost = user_state["buy_price"] * user_state["quantity"]
    
    await message.answer(
        f"Ценная бумага добавлена в портфель:\n\n"
        f"Тикер: {user_state['ticker']}\n"
        f"Компания: {company_name}\n"
        f"Цена покупки: {user_state['buy_price']:.2f} {currency}\n"
        f"Количество: {user_state['quantity']}\n"
        f"Общая стоимость: {total_cost:.2f} {currency}\n"
        f"Дата покупки: {buy_date}",
        reply_markup=portfolio_kb
    )
    
    user_states[user_id] = {"mode": "portfolio", "awaiting_ticker": False}

async def clear_news_cache():
    while True:
        await asyncio.sleep(3600)
        current_time = datetime.now()
        expired_keys = [key for key, timestamp in news_cache.items() 
                       if (current_time - timestamp).total_seconds() > 2592000]
        for key in expired_keys:
            del news_cache[key]

async def main():
    try:
        print("Бот запущен...")
        asyncio.create_task(clear_news_cache())
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        print(f"Ошибка при запуске бота: {e}")
        await asyncio.sleep(10)
        await main()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен")