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

TOKEN = "your_token"
FINNHUB_KEY = "your_key"

bot = Bot(TOKEN)
dp = Dispatcher()

user_states = {}
news_cache = {}

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Котировки"), KeyboardButton(text="Новости")],
        [KeyboardButton(text="Помощь")]
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

back_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Назад в меню")]
    ],
    resize_keyboard=True
)


def moex_detailed_quote(ticker, security_type="акция"):
    try:
        if security_type == "акция":
            market, board = "shares", "TQBR"
        else:
            # Для облигаций используем правильные параметры
            market, board = "bonds", "TQCB"  # Основная площадка для облигаций
        
        url = f"https://iss.moex.com/iss/engines/stock/markets/{market}/boards/{board}/securities/{ticker}.json"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # Проверяем, есть ли данные
            if not data.get("securities", {}).get("data"):
                # Пробуем альтернативные площадки для облигаций
                if security_type == "облигация":
                    alternative_boards = ["TQOB", "TQOD", "TQIR", "TQIN"]
                    for alt_board in alternative_boards:
                        alt_url = f"https://iss.moex.com/iss/engines/stock/markets/bonds/boards/{alt_board}/securities/{ticker}.json"
                        alt_response = requests.get(alt_url, timeout=5)
                        if alt_response.status_code == 200:
                            alt_data = alt_response.json()
                            if alt_data.get("securities", {}).get("data"):
                                data = alt_data
                                board = alt_board
                                break
            
            if (data.get("securities", {}).get("data") and 
                data.get("marketdata", {}).get("data")):
                
                security_info = data["securities"]["data"][0]
                market_data = data["marketdata"]["data"][0]
                
                company_name = security_info[2] if len(security_info) > 2 else ticker
                
                # Основная цена
                last_price = market_data[12] if market_data[12] is not None else market_data[4]
                
                # Цена предыдущего закрытия
                prev_close = security_info[11] if len(security_info) > 11 and security_info[11] is not None else last_price
                
                if last_price is None or not isinstance(last_price, (int, float)):
                    return None
                    
                if prev_close is None or not isinstance(prev_close, (int, float)):
                    prev_close = last_price
                
                # Расчет изменения к предыдущему дню
                if prev_close and last_price and prev_close > 0:
                    change_percent = ((last_price - prev_close) / prev_close) * 100
                    change_amount = last_price - prev_close
                else:
                    change_percent = 0
                    change_amount = 0
                
                return {
                    'company_name': company_name,
                    'ticker': ticker,
                    'price': last_price,
                    'change_percent': change_percent,
                    'change_amount': change_amount,
                    'prev_close': prev_close,
                    'security_type': security_type,
                    'currency': 'RUB'
                }
        
        return None
        
    except Exception as e:
        print(f"MOEX error: {e}")
        return None


def search_moex_bond(ticker):
    """Поиск облигации по ISIN или тикеру"""
    try:
        search_url = f"https://iss.moex.com/iss/securities.json?q={ticker}"
        response = requests.get(search_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            securities = data.get("securities", {}).get("data", [])
            
            for security in securities:
                sec_id = security[0] if security else None
                sec_name = security[2] if len(security) > 2 else ""
                sec_type = security[1] if len(security) > 1 else ""
                
                # Ищем облигации
                if sec_type in ["bond", "b", "ob"] and (ticker.upper() in sec_id.upper() or ticker.upper() in sec_name.upper()):
                    return sec_id
            
        return None
    except Exception as e:
        print(f"Bond search error: {e}")
        return None


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
        
        return {
            'company_name': company_name,
            'ticker': ticker,
            'price': current_price,
            'change_percent': change_percent,
            'change_amount': change_amount,
            'prev_close': previous_close,
            'security_type': 'акция',
            'currency': 'USD'
        }
        
    except Exception as e:
        print(f"Finnhub error: {e}")
        return None


def create_simple_chart(quote_data, security_type):
    try:
        plt.figure(figsize=(10, 6))
        
        # Простой график с двумя точками: предыдущее закрытие и текущая цена
        times = ['Пред. закрытие', 'Текущая цена']
        prices = [quote_data['prev_close'], quote_data['price']]
        
        plt.plot(times, prices, marker='o', linewidth=2, markersize=8, 
                color='red' if quote_data['change_percent'] < 0 else 'green')
        
        # Добавляем значения на график
        for i, (time, price) in enumerate(zip(times, prices)):
            plt.annotate(f'{price:.2f}', (time, price), textcoords="offset points", 
                        xytext=(0,10), ha='center', fontsize=9)
        
        plt.title(f'{quote_data["ticker"]} ({security_type})')
        plt.ylabel('Цена')
        plt.grid(True, alpha=0.3)
        
        if quote_data['change_percent'] < 0:
            plt.gca().set_facecolor('#fff5f5')
        else:
            plt.gca().set_facecolor('#f5fff5')
        
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=80, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        return buf
        
    except Exception as e:
        print(f"Chart error: {e}")
        return None


def get_smartlab_news_by_ticker(ticker):
    """Получение новостей с Smart-lab.ru по тикеру"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        search_url = f"https://smart-lab.ru/q/{ticker}/f/forum/"
        response = requests.get(search_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        news_items = []
        
        # Ищем посты на форуме
        posts = soup.find_all('div', class_=['message', 'post'])
        
        for post in posts[:5]:
            try:
                title_elem = post.find(['a', 'h3'])
                if title_elem:
                    title = title_elem.get_text().strip()
                    link = title_elem.get('href', '')
                    
                    if link and not link.startswith('http'):
                        link = f"https://smart-lab.ru{link}"
                    
                    # Ищем дату поста
                    date_elem = post.find(['span', 'div'], class_=['date', 'time'])
                    time_text = date_elem.get_text().strip() if date_elem else ""
                    
                    news_key = f"smartlab_{hash(title)}"
                    
                    if news_key not in news_cache:
                        news_cache[news_key] = datetime.now()
                        
                        news_items.append({
                            'title': title,
                            'link': link,
                            'time': time_text,
                            'source': 'Smart-lab',
                            'key': news_key
                        })
            except Exception:
                continue
                
        return news_items
        
    except Exception as e:
        print(f"Smart-lab news error: {e}")
        return []


def get_ria_news_by_ticker(ticker):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        search_url = f"https://ria.ru/search/?query={ticker}"
        response = requests.get(search_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        news_items = []
        
        articles = soup.find_all('div', class_=['list-item', 'search-item'])
        
        for article in articles[:3]:
            try:
                title_elem = article.find(['a', 'div'], class_=['list-item__title', 'search-item__title'])
                link_elem = article.find('a', href=True)
                
                if title_elem and link_elem:
                    title = title_elem.get_text().strip()
                    link = link_elem['href']
                    
                    if link.startswith('/'):
                        link = f"https://ria.ru{link}"
                    
                    time_elem = article.find('div', class_=['list-item__date', 'search-item__date'])
                    time_text = time_elem.get_text().strip() if time_elem else ""
                    
                    news_key = f"ria_ticker_{hash(title)}"
                    
                    if news_key not in news_cache:
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
        print(f"RIA ticker news error: {e}")
        return []


def get_ria_news(category="economic"):
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
        
        articles = soup.find_all('div', class_=['list-item', 'cell-list__item'])
        
        for article in articles[:8]:
            try:
                title_elem = article.find(['a', 'div'], class_=['list-item__title', 'cell-list__item-title'])
                link_elem = article.find('a', href=True)
                
                if title_elem and link_elem:
                    title = title_elem.get_text().strip()
                    link = link_elem['href']
                    
                    if link.startswith('/'):
                        link = f"https://ria.ru{link}"
                    
                    time_elem = article.find('div', class_=['list-item__date', 'cell-list__item-date'])
                    time_text = time_elem.get_text().strip() if time_elem else ""
                    
                    news_key = f"ria_{hash(title)}"
                    
                    if news_key not in news_cache:
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
                
        return news_items[:5]
        
    except Exception as e:
        print(f"RIA news error: {e}")
        return []


def get_tass_news(category="economic"):
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
        
        articles = soup.find_all('div', class_=['news-card', 'card-news'])
        
        for article in articles[:8]:
            try:
                title_elem = article.find(['a', 'h3'], class_=['news-card__title', 'card-news__title'])
                link_elem = article.find('a', href=True)
                
                if title_elem and link_elem:
                    title = title_elem.get_text().strip()
                    link = link_elem['href']
                    
                    if link.startswith('/'):
                        link = f"https://tass.ru{link}"
                    
                    news_key = f"tass_{hash(title)}"
                    
                    if news_key not in news_cache:
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
                
        return news_items[:5]
        
    except Exception as e:
        print(f"TASS news error: {e}")
        return []


def get_economic_news():
    try:
        sources = [
            get_ria_news("economic"),
            get_tass_news("economic")
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


def get_political_news():
    try:
        sources = [
            get_ria_news("political"),
            get_tass_news("political")
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
        sources = [
            get_smartlab_news_by_ticker(ticker),
            get_ria_news_by_ticker(ticker)
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
        
        if unique_news:
            return unique_news[:3]
        
        return [{
            'title': f'Новости по {ticker} будут доступны в ближайшее время',
            'link': f'https://smart-lab.ru/q/{ticker}/',
            'time': 'Сегодня',
            'source': 'Smart-lab',
            'key': f'fallback_{ticker}_{datetime.now().timestamp()}'
        }]
        
    except Exception as e:
        print(f"Ticker news error: {e}")
        return []


@dp.message(Command("start"))
async def start(message: Message):
    user_id = message.from_user.id
    user_states[user_id] = {"mode": None, "awaiting_ticker": False}
    
    welcome_text = """
Добро пожаловать в бот для отслеживания котировок и новостей!

Основные функции:
- Котировки российских и зарубежных ценных бумаг
- Актуальные экономические и политические новости
- Новости по конкретным компаниям

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

Примеры тикеров:
Российские акции: SBER, GAZP, VTBR, YNDX
Российские облигации: SU26230, RU000A105C99
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


@dp.message()
async def handle_message(message: Message):
    user_id = message.from_user.id
    user_state = user_states.get(user_id, {"mode": None, "awaiting_ticker": False})
    
    if (message.text.startswith('/') or 
        message.text in ["Котировки", "Новости", "Помощь", "Назад в меню",
                        "Российские ценные бумаги", "Зарубежные ценные бумаги",
                        "Российские акции", "Российские облигации",
                        "Экономические новости", "Политические новости", "Новости по тикеру"]):
        return
    
    if user_state.get("awaiting_ticker"):
        ticker = message.text.upper().strip()
        
        if not ticker:
            await message.answer("Введите тикер:")
            return
        
        if user_state.get("mode") == "securities":
            securities_type = user_state.get("securities_type")
            security_subtype = user_state.get("security_subtype", "акция")
            
            await message.answer(f"Поиск котировок для {ticker}...")
            
            if securities_type == "moex":
                # Для облигаций сначала ищем правильный идентификатор
                if security_subtype == "облигация":
                    found_ticker = search_moex_bond(ticker)
                    if found_ticker:
                        ticker = found_ticker
                
                quote_data = moex_detailed_quote(ticker, security_subtype)
                src = "Московская биржа"
                chart_link = f"https://www.moex.com/ru/issue.aspx?board=TQBR&code={ticker}" if security_subtype == "акция" else f"https://www.moex.com/ru/issue.aspx?board=TQCB&code={ticker}"
            else:
                quote_data = finnhub_detailed_quote(ticker)
                src = "Международные биржи"
                chart_link = f"https://www.tradingview.com/symbols/{ticker}/"

            if not quote_data:
                await message.answer(f"Тикер {ticker} не найден на {src}. Проверьте правильность тикера.")
                return

            price = quote_data['price']
            change_percent = quote_data['change_percent']
            change_amount = quote_data['change_amount']
            
            if price is not None and isinstance(price, (int, float)) and price > 0:
                formatted_price = f"{price:.2f}"
            else:
                formatted_price = "Н/Д"
            
            currency_symbol = "RUB" if quote_data['currency'] == 'RUB' else "USD"
            
            result_text = f"""
Компания: {quote_data['company_name']}
Тикер: {ticker}
Тип: {quote_data['security_type']}
Биржа: {src}

Текущая цена: {formatted_price} {currency_symbol}
Предыдущее закрытие: {quote_data['prev_close']:.2f} {currency_symbol}
Изменение: {change_amount:+.2f} {currency_symbol} ({change_percent:+.2f}%)

Подробный график: {chart_link}

Обновлено: {datetime.now().strftime('%H:%M %d.%m.%Y')}
"""
            await message.answer(result_text)
            
            chart_buf = create_simple_chart(quote_data, src)
            if chart_buf:
                photo = BufferedInputFile(chart_buf.getvalue(), filename=f"chart_{ticker}.png")
                await message.answer_photo(photo, caption=f"График {ticker}")
        
        elif user_state.get("mode") == "news" and user_state.get("news_type") == "ticker":
            await message.answer(f"Поиск новостей по {ticker}...")
            
            news_items = get_news_by_ticker(ticker)
            
            if not news_items:
                await message.answer(f"Новости по {ticker} не найдены.")
                return
            
            news_text = f"Новости по {ticker}:\n\n"
            
            for i, item in enumerate(news_items, 1):
                news_text += f"{i}. {item['title']}\n"
                if item.get('time'):
                    news_text += f"   Время: {item['time']}\n"
                news_text += f"   Источник: {item['source']}\n"
                news_text += f"   Ссылка: {item['link']}\n\n"
            
            await message.answer(news_text[:4096])
    
    else:
        await message.answer("Выберите действие из меню.", reply_markup=main_kb)


async def clear_news_cache():
    while True:
        await asyncio.sleep(3600)
        current_time = datetime.now()
        expired_keys = [key for key, timestamp in news_cache.items() 
                       if (current_time - timestamp).total_seconds() > 7200]
        for key in expired_keys:
            del news_cache[key]


async def main():
    try:
        print("Бот запущен...")
        asyncio.create_task(clear_news_cache())
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        print(f"Ошибка при запуске бота: {e}")
        await asyncio.sleep(5)
        await main()  # Перезапуск при ошибке


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен")