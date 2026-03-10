#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HDRezka Downloader v4.6.5 //Уверенная База//
✨ Прогресс-бар (Ubuntu-style) + HLS + 3 метода озвучек + Многопоточность
+ Правильное распознавание названия + Динамический поиск озвучек (--sca-id)
+ Анализ сезонов/серий для каждой озвучки + Диапазоны сезонов (-s "1-3") + Range + Авторизация
"""
from HdRezkaApi import HdRezkaApi
import requests, os, argparse, sys, time, re, ast, math, json, subprocess, ctypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

# ================= НАСТРОЙКИ =================
DEFAULT_THREADS = 8
MAX_THREADS = 20
CHUNK_SIZE = 1024 * 1024
TIMEOUT = 30
MAX_RETRIES = 3
HLS_THREADS = 4
DEFAULT_SCAN_ID = 1000  # ✅ По умолчанию сканируем до ID 1000
# =============================================

# ANSI цвета
COLOR_GREEN = '\033[92m'
COLOR_YELLOW = '\033[93m'
COLOR_BLUE = '\033[94m'
COLOR_RED = '\033[91m'
COLOR_CYAN = '\033[96m'
COLOR_MAGENTA = '\033[95m'
COLOR_RESET = '\033[0m'
COLOR_BOLD = '\033[1m'

# Глобальные переменные
content_title = 'Unknown'
content_type = 'movie'

# ================= БАННЕР СТИЛЯ #3 =================

def print_confident_base_banner():
    
    print(f"{COLOR_BLUE}")
    print(r"""
    __    __   __   __   __   __   __   __       __   __   __   __
   / /   / /  / /  / /  / /  / /  / /  / /      / /  / /  / /  / /
  / /   / /  / /  / /  / /  / /  / /  / /      / /  / /  / /  / /
 / /   / /  / /  / /  / /  / /  / /  / /      / /  / /  / /  / /
/ /___/ /__/ /__/ /__/ /__/ /__/ /__/ /______/ /__/ /__/ /__/ /__
\_______________________________/    \___________________________/
         Уверенная                           База
    """)
    print(f"{COLOR_RESET}")

# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================

def enable_ansi_support():
    """Включает поддержку ANSI-кодов в Windows CMD/PowerShell"""
    if sys.platform == 'win32':
        try:
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
        except:
            pass

def plural_ru(number, one, few, many):
    """
    ✅ ПРАВИЛЬНОЕ СКЛОНЕНИЕ СЛОВ В РУССКОМ ЯЗЫКЕ
    """
    n = abs(number) % 100
    n1 = n % 10
    if n > 10 and n < 20:
        return many
    if n1 > 1 and n1 < 5:
        return few
    if n1 == 1:
        return one
    return many

def parse_range(range_str, max_value=100):
    """
    ✅ ПАРСИТ ДИАПАЗОНЫ: "1-3" → [1,2,3], "1,3,5" → [1,3,5], "1-3,5" → [1,2,3,5]
    """
    if not range_str:
        return None
    result = set()
    parts = range_str.split(',')
    for part in parts:
        part = part.strip()
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                result.update(range(start, min(end, max_value) + 1))
            except ValueError:
                continue
        else:
            try:
                result.add(int(part))
            except ValueError:
                continue
    return sorted(result) if result else None

def parse_content_info(url, headers):
    """✅ УЛУЧШЕННЫЙ ПАРСЕР — лучше определяет сериалы"""
    global content_title, content_type
    try:
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # 🔧 Метод 1: h1 с классом entry-title
        h1 = soup.find('h1', class_='entry-title')
        if h1:
            title = h1.get_text(strip=True)
            title = re.split(r'\s*[-–|]\s*(?:смотреть|онлайн|HDRezka).*$', title, flags=re.I)[0].strip()
            title = re.sub(r'\s*\(\d{4}\)\s*$', '', title)
            if title and len(title) > 2:
                content_title = title
        
        # 🔧 Метод 2: Из URL
        if content_title == 'Unknown':
            match = re.search(r'/\d+-([^.]+)\.html$', url)
            if match:
                title_raw = match.group(1)
                words = title_raw.split('-')
                if words and re.match(r'^\d{4}$', words[-1]):
                    words = words[:-1]
                content_title = ' '.join(word.capitalize() for word in words)
        
        # 🔧 УЛУЧШЕННОЕ определение типа контента
        url_lower = url.lower()
        page_text = soup.get_text().lower()
        
        # 🎬 Явные признаки фильма
        movie_keywords = ['полнометражный', 'полный метр', 'movie', 'film']
        # 📺 Явные признаки сериала
        series_keywords = [
            'сезон', 'серия', 'серии', 'эпизод', 'season', 'episode',
            'список серий', 'все серии', 'серий', 'эпизодов',
            'аниме', 'анимация', 'animation', 'tv series'
        ]
        # 🎞 Короткометражки
        short_keywords = ['short', 'короткометра', 'байки', 'mini', 'bayki']
        
        # 🎯 Приоритет: если есть признаки сериала — это сериал
        if any(kw in url_lower or kw in page_text for kw in series_keywords):
            content_type = 'series'
        elif any(kw in url_lower or kw in page_text for kw in short_keywords):
            content_type = 'shorts'
        elif any(kw in url_lower or kw in page_text for kw in movie_keywords):
            content_type = 'movie'
        # 🔄 Фолбэк на старую логику
        elif soup.find(class_='seasons-list') or soup.find(id='seasons') or 'season' in url_lower:
            content_type = 'series'
        elif soup.find(class_='movie-player') or 'film' in url_lower or 'movie' in url_lower:
            content_type = 'movie'
        else:
            content_type = 'movie'
        
        return content_title, content_type
    except Exception as e:
        print(f"{COLOR_YELLOW}⚠️  Предупреждение парсера: {e}{COLOR_RESET}")
        match = re.search(r'/\d+-([^.]+)\.html$', url)
        if match:
            content_title = match.group(1).replace('-', ' ').title()
        return content_title, content_type

def parse_episodes(episodes_str):
    """Парсинг строки серий: '1-10', '1,3,5'"""
    if not episodes_str:
        return None
    result = set()
    parts = episodes_str.split(',')
    for part in parts:
        part = part.strip()
        if '-' in part:
            start, end = map(int, part.split('-'))
            result.update(range(start, end + 1))
        else:
            result.add(int(part))
    return sorted(result)

def extract_single_url(raw_url):
    """Извлекает одну рабочую ссылку из возврата HdRezkaApi"""
    if not raw_url:
        return None
    if isinstance(raw_url, str) and raw_url.strip().startswith('http'):
        return raw_url.strip()
    if isinstance(raw_url, list):
        for item in raw_url:
            cleaned = str(item).strip().strip("'\"")
            if cleaned.startswith('http'):
                return cleaned
        return None
    raw_str = str(raw_url).strip()
    if raw_str.startswith('[') and ']' in raw_str:
        try:
            urls = ast.literal_eval(raw_str)
            if isinstance(urls, list):
                for item in urls:
                    cleaned = str(item).strip().strip("'\"")
                    if cleaned.startswith('http'):
                        return cleaned
        except:
            pass
    if ' or ' in raw_str:
        parts = raw_str.split(' or ')
        for part in parts:
            cleaned = part.strip().strip("'\"[]")
            if cleaned.startswith('http'):
                return cleaned
    match = re.search(r'(https?://[^\s\'"\]]+)', raw_str)
    if match:
        return match.group(1).strip()
    return None

def is_hls_url(url):
    """Проверяет, является ли ссылка HLS потоком"""
    if not url:
        return False
    return '.m3u8' in url.lower() or 'hls' in url.lower() or 'manifest' in url.lower()

def parse_m3u8_playlist(url, headers):
    """Парсит HLS плейлист и возвращает список сегментов"""
    try:
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        segments = []
        base_url = '/'.join(url.split('/')[:-1]) + '/'
        for line in resp.text.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and line.endswith('.ts'):
                if line.startswith('http'):
                    segments.append(line)
                else:
                    segments.append(base_url + line)
        return segments
    except Exception as e:
        print(f"  {COLOR_RED}❌ Ошибка парсинга HLS: {e}{COLOR_RESET}")
        return None

def download_hls_segment(segment_url, segment_id, output_dir, headers):
    """Скачивает один HLS сегмент"""
    try:
        resp = requests.get(segment_url, headers=headers, timeout=TIMEOUT, stream=True)
        resp.raise_for_status()
        filename = os.path.join(output_dir, f"seg_{segment_id:05d}.ts")
        with open(filename, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True, segment_id
    except Exception as e:
        return False, segment_id, str(e)

def download_hls_stream(url, filename, headers, total_threads=HLS_THREADS):
    """Скачивает HLS поток (m3u8)"""
    print(f"  {COLOR_BLUE}📺 Обнаружен HLS поток (.m3u8){COLOR_RESET}")
    print(f"  🧵 Потоков для сегментов: {total_threads}")
    temp_dir = filename + "_hls_temp"
    os.makedirs(temp_dir, exist_ok=True)
    print("  🔍 Парсинг плейлиста...")
    segments = parse_m3u8_playlist(url, headers)
    if not segments:
        print("  ❌ Не удалось получить список сегментов")
        return False
    total_segments = len(segments)
    print(f"  📦 Найдено сегментов: {total_segments}")
    start_time = time.time()
    downloaded = 0
    print(f"  {COLOR_BLUE}{'='*60}{COLOR_RESET}")
    with ThreadPoolExecutor(max_workers=total_threads) as executor:
        futures = {
            executor.submit(download_hls_segment, seg, idx, temp_dir, headers): idx
            for idx, seg in enumerate(segments)
        }
        for future in as_completed(futures):
            success = future.result()
            if success[0]:
                downloaded += 1
                elapsed = time.time() - start_time
                speed = downloaded / elapsed if elapsed > 0 else 0
                percent = downloaded / total_segments * 100
                bar_width = 50
                filled = int(bar_width * downloaded / total_segments)
                bar = f"{COLOR_GREEN}|{COLOR_RESET}" * filled + "░" * (bar_width - filled)
                print(f'\r  {COLOR_GREEN}[{bar}]{COLOR_RESET} {percent:5.1f}% | ⚡ {speed:5.1f} сег/с | {downloaded}/{total_segments}{COLOR_RESET}', end="", flush=True)
            else:
                print(f"\n  {COLOR_RED}❌ Ошибка сегмента {success[1]}: {success[2]}{COLOR_RESET}")
    print(f"\n  {COLOR_BLUE}{'='*60}{COLOR_RESET}")
    print("  🔗 Объединяю сегменты...")
    try:
        has_ffmpeg = subprocess.run(['ffmpeg', '-version'], capture_output=True).returncode == 0
        if has_ffmpeg:
            concat_file = os.path.join(temp_dir, "concat.txt")
            with open(concat_file, 'w', encoding='utf-8') as f:
                for i in range(total_segments):
                    f.write(f"file 'seg_{i:05d}.ts'\n")
            subprocess.run([
                'ffmpeg', '-y', '-f', 'concat', '-safe', '0',
                '-i', concat_file, '-c', 'copy', filename
            ], capture_output=True)
        else:
            with open(filename, 'wb') as outfile:
                for i in range(total_segments):
                    seg_file = os.path.join(temp_dir, f"seg_{i:05d}.ts")
                    if os.path.exists(seg_file):
                        with open(seg_file, 'rb') as infile:
                            outfile.write(infile.read())
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
        elapsed = time.time() - start_time
        print(f"  {COLOR_GREEN}✅ Готово:{COLOR_RESET} {os.path.basename(filename)} | ⏱ {elapsed:.1f} сек")
        return True
    except Exception as e:
        print(f"  {COLOR_RED}❌ Ошибка объединения: {e}{COLOR_RESET}")
        return False

def format_size(size_bytes):
    """Форматирует размер в человекочитаемый вид"""
    for unit in ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} ПБ"

def format_time(seconds):
    """Форматирует время в человекочитаемый вид"""
    if seconds < 60:
        return f"{seconds:.0f} сек             "
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins} мин {secs} сек         "
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours} ч {mins} мин          "

def print_progress_bar(current, total, prefix='Прогресс', suffix='', length=50):
    """
    ✅ ПРОГРЕСС-БАР КАК В v4.0 — ОДНА СТРОКА ПЕРЕЗАПИСЫВАЕТСЯ
    """
    if total == 0:
        return
    percent = 100 * (current / float(total))
    filled_length = int(length * current // total)
    bar = f"{COLOR_GREEN}|{COLOR_RESET}" * filled_length + "░" * (length - filled_length)
    sys.stdout.write('\r' + ' ' * 100 + '\r')
    sys.stdout.flush()
    output = f'{COLOR_BLUE}{prefix}{COLOR_RESET} [{bar}] {COLOR_BOLD}{percent:5.1f}%{COLOR_RESET} {suffix}'
    sys.stdout.write(output)
    sys.stdout.flush()

# ================= ОЗВУЧКИ =================

def get_translations_from_api(rezka):
    """МЕТОД 1: Получение озвучек через API"""
    try:
        if hasattr(rezka, 'getInfo'):
            info = rezka.getInfo()
            if info and 'translations' in info:
                translations = [(t['id'], t['name']) for t in info['translations']]
                return translations
        if hasattr(rezka, 'translations'):
            translations = rezka.translations
            if translations:
                return translations
        return None
    except:
        return None

def get_translations_from_html(rezka, url, season=1, episode=1):
    """МЕТОД 2: Парсинг озвучек из HTML"""
    try:
        headers = {
            "Referer": "https://hdrezka2vbppy.org/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        translations = []
        translation_selectors = [
            '#translation-list', '.translation-list', '[data-module="CDNPlayer"]',
            '.b-simple-select__list', '#player-translations', '.voices-select', '#voices'
        ]
        for selector in translation_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    options = element.find_all(['option', 'button', 'a', 'div'],
                                              class_=re.compile(r'translation|voice|audio', re.I))
                    for opt in options:
                        t_id = opt.get('value') or opt.get('data-id') or opt.get('data-translation')
                        t_name = opt.get_text(strip=True) or opt.get('title', '')
                        if t_id and t_name:
                            try:
                                t_id = int(t_id)
                                translations.append((t_id, t_name.strip()))
                            except:
                                pass
                    if translations:
                        break
            except:
                continue
        if not translations:
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'translation' in script.string.lower():
                    matches = re.findall(r'\{\s*["\']id["\']\s*:\s*(\d+)\s*,\s*["\']name["\']\s*:\s*["\']([^"\']+)["\']', script.string)
                    for t_id, t_name in matches:
                        translations.append((int(t_id), t_name.strip()))
        translations = list(dict(translations).items())
        return translations if translations else None
    except:
        return None

def get_translations_by_scanning(rezka, season=1, episode=1, max_id=1000):
    """
    ✅ МЕТОД 3: Тихое сканирование ID переводов (ДИНАМИЧЕСКИЙ ДИАПАЗОН)
    """
    translations = []
    
    # ✅ РАСШИРЕННЫЙ СЛОВАРЬ ИЗВЕСТНЫХ ОЗВУЧЕК
    known_names = {
        1: "Дубляж", 2: "Многоголосый закадровый", 3: "Двухголосый закадровый", 
        4: "Одноголосый закадровый", 5: "Субтитры", 6: "VO (Voice Over)",
        7: "LostFilm", 8: "AlexFilm", 9: "ColdFilm", 10: "StudioBand",
        11: "Kerob", 12: "Jaskier", 13: "Red Head Sound", 14: "AniLibria",
        15: "Studio 8", 16: "Green Ray", 17: "Flarrow Films", 18: "Horror Place",
        19: "Кубик в Кубе", 20: "Sony Pictures", 21: "Paramount", 22: "WB TV",
        23: "Showcase", 24: "BBC", 25: "Netflix", 26: "HBO", 27: "Amazon Prime",
        28: "Disney+", 29: "Apple TV+", 30: "Hulu",
        31: "СТС", 32: "НТВ", 33: "ТНТ", 34: "Первый канал", 35: "Россия 1",
        36: "Пятница!", 37: "2x2", 38: "ТВ-3", 39: "Муз-ТВ", 40: "MTV Россия",
        41: "REN TV", 42: "5 канал", 43: "Звезда", 44: "Мир", 45: "Дождь",
        46: "RTVI", 47: "Карусель", 48: "Матч ТВ", 49: "Суббота!", 50: "Че",
        56: "Дубляж", 81: "Многоголосый", 238: "Перевод #238",
        100: "Украинский дубляж", 101: "Украинский многоголосый", 102: "Украинский одноголосый",
        200: "Режиссёрская версия", 201: "Дубляж (режиссёрская версия)", 202: "Многоголосый (режиссёрская версия)",
        999: "Оригинал (+субтитры)",
    }
    
    # 🔇 Тихое сканирование — без вывода каждого ID
    for t_id in range(1, max_id + 1):
        try:
            stream = rezka.getStream(season=season, episode=episode, translation=t_id)
            url = stream('480p')
            if url and extract_single_url(url):
                translation_name = known_names.get(t_id, f"Перевод #{t_id}")
                translations.append((t_id, translation_name))
        except:
            pass
    
    return translations

def count_episodes_in_season(rezka, season, translation_id, max_check=100):
    """
    ✅ НАДЁЖНЫЙ ПОДСЧЁТ СЕРИЙ В СЕЗОНЕ (метод из --all)
    """
    for ep in range(max_check, 0, -1):
        try:
            stream = rezka.getStream(season=season, episode=ep, translation=translation_id)
            url = stream('480p')
            if url and extract_single_url(url):
                return ep
        except:
            continue
    return 0

def analyze_translation_coverage(rezka, translation_id, max_seasons=20, max_episodes=100):
    """
    ✅ АНАЛИЗ ДОСТУПНОСТИ СЕЗОНОВ И СЕРИЙ (через надёжный метод --all)
    """
    seasons_data = {}
    for season in range(1, max_seasons + 1):
        try:
            stream = rezka.getStream(season=season, episode=1, translation=translation_id)
            url = stream('480p')
            if not (url and extract_single_url(url)):
                found = False
                for ep in [1, 2, 3]:
                    try:
                        stream = rezka.getStream(season=season, episode=ep, translation=translation_id)
                        url = stream('480p')
                        if url and extract_single_url(url):
                            found = True
                            break
                    except:
                        continue
                if not found:
                    break
        except:
            break
        total_eps = count_episodes_in_season(rezka, season, translation_id, max_episodes)
        if total_eps > 0:
            seasons_data[season] = list(range(1, total_eps + 1))
        else:
            break
    return seasons_data

def print_seasons_analysis(translations, rezka, max_seasons=20, max_episodes=100):
    """
    ✅ ВЫВОДИТ АНАЛИЗ СЕЗОНОВ/СЕРИЙ ДЛЯ ВСЕХ ОЗВУЧЕК
    """
    print(f"\n{COLOR_BLUE}{'='*70}{COLOR_RESET}")
    print(f"{COLOR_BOLD}📊 АНАЛИЗ ДОСТУПНЫХ СЕЗОНОВ И СЕРИЙ{COLOR_RESET}")
    print(f"{COLOR_BLUE}{'='*70}{COLOR_RESET}\n")
    
    if content_type not in ('series', 'shorts'):
        print(f"{COLOR_YELLOW}ℹ️  Контент не является сериалом или короткометражкой{COLOR_RESET}")
        print(f"   Анализ сезонов применим только к сериалам/короткометражкам.\n")
        return
    
    print(f"🔍 Проверяю доступность сезонов для каждой озвучки...")
    print(f"   (используется надёжный метод обратного поиска, как в --all)\n")
    
    results = {}
    for t_id, t_name in translations:
        if t_id is None:
            continue
        print(f"   ⏳ {t_name} (ID {t_id})...", end="", flush=True)
        seasons_data = analyze_translation_coverage(rezka, t_id, max_seasons, max_episodes)
        results[t_id] = {'name': t_name, 'seasons': seasons_data}
        
        if seasons_data:
            total_seasons = len(seasons_data)
            total_episodes = sum(len(eps) for eps in seasons_data.values())
            season_word = plural_ru(total_seasons, "сезон", "сезона", "сезонов")
            episode_word = plural_ru(total_episodes, "серия", "серии", "серий")
            print(f" {COLOR_GREEN}✅{COLOR_RESET} {total_seasons} {season_word}, {total_episodes} {episode_word}")
        else:
            print(f" {COLOR_RED}❌{COLOR_RESET} нет доступных серий")
    
    print(f"\n{COLOR_BOLD}📋 СВОДНАЯ ТАБЛИЦА:{COLOR_RESET}\n")
    print(f"{'Озвучка':<35} {'Сезоны':<20} {'Всего серий':<15}")
    print("-"*70)
    for t_id, data in sorted(results.items(), key=lambda x: x[0]):
        t_name = data['name']
        seasons_data = data['seasons']
        if seasons_data:
            total_episodes = sum(len(eps) for eps in seasons_data.values())
            season_range = f"{min(seasons_data.keys())}-{max(seasons_data.keys())}"
            print(f"{COLOR_CYAN}{t_name:<35}{COLOR_RESET} {season_range:<20} {total_episodes:<15}")
        else:
            print(f"{COLOR_CYAN}{t_name:<35}{COLOR_RESET} {'—':<20} {'0':<15}")
    print("-"*70)
    
    print(f"\n{COLOR_BOLD}🔍 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ:{COLOR_RESET}\n")
    for t_id, data in sorted(results.items(), key=lambda x: x[0]):
        t_name = data['name']
        seasons_data = data['seasons']
        if not seasons_data:
            continue
        print(f"{COLOR_BOLD}{t_name} (ID {t_id}):{COLOR_RESET}")
        for season in sorted(seasons_data.keys()):
            episodes = seasons_data[season]
            ep_count = len(episodes)
            ep_range = f"{episodes[0]}-{episodes[-1]}" if len(episodes) > 1 else str(episodes[0])
            episode_word = plural_ru(ep_count, "серия", "серии", "серий")
            print(f"   📺 Сезон {season}: {ep_count} {episode_word} ({ep_range})")
        print()

def get_translations_from_page(rezka, url, season=1, episode=1, scan_max_id=1000):
    """Комбинирует все 3 метода"""
    translations = get_translations_from_api(rezka)
    if translations:
        return translations, "API"
    translations = get_translations_from_html(rezka, url, season, episode)
    if translations:
        return translations, "HTML"
    # ✅ ДИНАМИЧЕСКИЙ ПОИСК ДО scan_max_id
    translations = get_translations_by_scanning(rezka, season, episode, max_id=scan_max_id)
    if translations:
        translations.insert(0, (None, "По умолчанию"))
        return translations, "SCAN"
    return [(None, "По умолчанию")], "NONE"

def save_translations_to_file(translations, output_file="translations.json"):
    """Сохраняет список озвучек в JSON"""
    data = {
        "timestamp": datetime.now().isoformat(),
        "translations": [{"id": t_id, "name": t_name} for t_id, t_name in translations]
    }
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Список сохранён в: {output_file}")

# ================= СКАЧИВАНИЕ =================

def test_range_support(url, headers):
    """Проверяет поддержку Range"""
    try:
        test_headers = headers.copy()
        test_headers['Range'] = 'bytes=0-1'
        resp = requests.get(url, headers=test_headers, timeout=TIMEOUT, stream=True)
        if resp.status_code == 206:
            return True
        elif resp.status_code == 200:
            content = b''
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    content += chunk
                    if len(content) >= 2:
                        break
            return len(content) == 2
        return False
    except:
        return None

def download_chunk(url, start, end, chunk_id, headers, output_path, force_range=False):
    """Скачивает один чанк"""
    chunk_headers = headers.copy()
    if force_range or start > 0 or end < float('inf'):
        chunk_headers['Range'] = f'bytes={start}-{end}'
    try:
        resp = requests.get(url, headers=chunk_headers, timeout=TIMEOUT, stream=True)
        if resp.status_code == 206:
            with open(f"{output_path}.part{chunk_id}", 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True, chunk_id
        elif resp.status_code == 200 and not force_range:
            return False, chunk_id, "Server doesn't support Range"
        elif resp.status_code == 200 and force_range:
            data = b''.join(resp.iter_content(chunk_size=8192))
            segment = data[start:end+1]
            with open(f"{output_path}.part{chunk_id}", 'wb') as f:
                f.write(segment)
            return True, chunk_id
        return False, chunk_id, f"Status {resp.status_code}"
    except Exception as e:
        return False, chunk_id, str(e)

def download_chunk_with_retry(url, start, end, chunk_id, headers, output_path, force_range=False):
    """Скачивает чанк с повторными попытками"""
    for attempt in range(MAX_RETRIES):
        success, cid, error = download_chunk(url, start, end, chunk_id, headers, output_path, force_range)
        if success:
            return True, chunk_id
        print(f"\n  {COLOR_YELLOW}🔄 Чанк {cid}: попытка {attempt+1}/{MAX_RETRIES}{COLOR_RESET}")
        time.sleep(2 ** attempt)
    return False, chunk_id, f"Failed after {MAX_RETRIES} attempts"

def merge_chunks(output_path, total_chunks):
    """Объединяет чанки"""
    try:
        with open(output_path, 'wb') as outfile:
            for i in range(total_chunks):
                chunk_file = f"{output_path}.part{i}"
                if os.path.exists(chunk_file):
                    with open(chunk_file, 'rb') as infile:
                        outfile.write(infile.read())
                    os.remove(chunk_file)
                else:
                    print(f"⚠️  Чанк {i} не найден!")
                    return False
        return True
    except Exception as e:
        print(f"❌ Ошибка объединения: {e}")
        return False

def download_single_thread(url, filename, headers, show_progress=True):
    """Однопоточное скачивание"""
    try:
        with requests.get(url, stream=True, headers=headers, timeout=TIMEOUT) as r:
            r.raise_for_status()
            total = int(r.headers.get('content-length', 0))
            downloaded = 0
            start_time = time.time()
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total and show_progress:
                            elapsed = time.time() - start_time
                            speed = (downloaded / 1024 / 1024) / elapsed if elapsed > 0 else 0
                            percent = downloaded / total * 100
                            eta = (total - downloaded) / (downloaded / elapsed) if elapsed > 0 and downloaded > 0 else 0
                            bar_width = 50
                            filled = int(bar_width * downloaded / total)
                            bar = f"{COLOR_GREEN}|{COLOR_RESET}" * filled + "░" * (bar_width - filled)
                            print(f'\r  {COLOR_GREEN}[{bar}]{COLOR_RESET} {COLOR_BOLD}{percent:5.1f}%{COLOR_RESET} | ⚡ {speed:5.1f} МБ/с | ⏱ {format_time(eta)}', end="", flush=True)
            print()
            return True
    except Exception as e:
        print(f"  {COLOR_RED}❌ Ошибка: {e}{COLOR_RESET}")
        return False

def download_multithreaded(url, filename, headers, total_threads=DEFAULT_THREADS, force_range=False, show_progress=True):
    """Многопоточное скачивание"""
    try:
        head = requests.head(url, headers=headers, timeout=TIMEOUT, allow_redirects=True)
        file_size = int(head.headers.get('Content-Length', 0))
        if file_size == 0:
            print("  ⚠️  Не удалось получить размер файла, однопоточный режим")
            return download_single_thread(url, filename, headers, show_progress)
        accept_ranges = head.headers.get('Accept-Ranges', '').lower()
        range_supported = (accept_ranges == 'bytes')
        if not range_supported and force_range:
            print("  🔍 Тестирую поддержку Range (force-mode)...")
            test_result = test_range_support(url, headers)
            if test_result is True:
                print("  ✅ Сервер принимает Range-запросы!")
                range_supported = True
            elif test_result is False:
                print("  ❌ Сервер не поддерживает Range, однопоточный режим")
                return download_single_thread(url, filename, headers, show_progress)
        if not range_supported and not force_range:
            print("  ⚠️  Сервер не поддерживает многопоточность, однопоточный режим")
            print("  💡 Попробуйте добавить --force-range")
            return download_single_thread(url, filename, headers, show_progress)
        print(f"  📦 Размер: {format_size(file_size)}")
        print(f"  🧵 Потоков: {total_threads}")
        chunk_size = math.ceil(file_size / total_threads)
        chunks = [(i*chunk_size, min((i+1)*chunk_size-1, file_size-1), i) for i in range(total_threads)]
        downloaded_chunks = 0
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=total_threads) as executor:
            futures = {
                executor.submit(download_chunk_with_retry, url, s, e, c, headers, filename, force_range): c
                for s, e, c in chunks
            }
            for future in as_completed(futures):
                success = future.result()
                if success[0]:
                    downloaded_chunks += 1
                    elapsed = time.time() - start_time
                    speed = (file_size / 1024 / 1024) / elapsed if elapsed > 0 else 0
                    percent = downloaded_chunks / total_threads * 100
                    eta = (total_threads - downloaded_chunks) / (downloaded_chunks / elapsed) if elapsed > 0 and downloaded_chunks > 0 else 0
                    bar_width = 50
                    filled = int(bar_width * downloaded_chunks / total_threads)
                    bar = f"{COLOR_GREEN}|{COLOR_RESET}" * filled + "░" * (bar_width - filled)
                    print(f'\r  {COLOR_GREEN}[{bar}]{COLOR_RESET} {COLOR_BOLD}{percent:5.1f}%{COLOR_RESET} | ⚡ {speed:5.1f} МБ/с | ⏱ {format_time(eta)}', end="", flush=True)
                else:
                    print(f"\n  {COLOR_RED}❌ Ошибка чанка {success[1]}: {success[2]}{COLOR_RESET}")
                    if "Range" in str(success[2]) or "206" in str(success[2]):
                        print("  🔄 Переключаюсь на однопоточный режим...")
                        return download_single_thread(url, filename, headers, show_progress)
                    return False
        print()
        print("  🔗 Объединяю чанки...")
        if merge_chunks(filename, total_threads):
            elapsed = time.time() - start_time
            speed = (file_size / 1024 / 1024) / elapsed if elapsed > 0 else 0
            print(f"  {COLOR_GREEN}✅ Готово:{COLOR_RESET} {os.path.basename(filename)} | ⚡ {speed:5.1f} МБ/с | ⏱ {format_time(elapsed)}")
            return True
        return False
    except Exception as e:
        print(f"  ⚠️  Ошибка многопоточного скачивания: {e}")
        print("  🔄 Переключаюсь на однопоточный режим...")
        return download_single_thread(url, filename, headers, show_progress)

def get_output_path(base_dir, title, quality, season, episode):
    """
    ✅ ИСПРАВЛЕННАЯ СТРУКТУРА ИМЁН (наследуется 4.6.x)
    """
    safe_title = title.replace('/', '_').replace('\\', '_').strip()
    if not safe_title:
        safe_title = 'Unknown'
    content_folder = f"{safe_title} [{quality}]"
    season_dir = os.path.join(base_dir, content_folder, f"S{season}")
    os.makedirs(season_dir, exist_ok=True)
    filename = f"{safe_title} [{quality}]_s{season:02d}e{episode:02d}.mp4"
    return os.path.join(season_dir, filename)

def download_episode(rezka, quality, season, episode, output_dir, headers, translation_id=None, force_range=False, threads=DEFAULT_THREADS, show_progress=True):
    """Скачивание одной серии"""
    try:
        stream = rezka.getStream(season=season, episode=episode, translation=translation_id)
        raw_url = stream(quality)
        video_url = extract_single_url(raw_url)
        if not video_url:
            print(f"  {COLOR_RED}⚠️  Не удалось извлечь ссылку для качества '{quality}'{COLOR_RESET}")
            return False
        filename = get_output_path(output_dir, content_title, quality, season, episode)
        if os.path.exists(filename) and os.path.getsize(filename) > 1024:
            print(f"  {COLOR_GREEN}✅ Уже скачан:{COLOR_RESET} {os.path.basename(filename)}")
            return True
        print(f"  ⬇️  Скачиваю: {COLOR_BOLD}{os.path.basename(filename)}{COLOR_RESET}")
        print(f"  🔗 URL: {video_url[:70]}...")
        if is_hls_url(video_url):
            success = download_hls_stream(video_url, filename, headers, HLS_THREADS)
        else:
            success = download_multithreaded(video_url, filename, headers, threads, force_range, show_progress)
        return success
    except Exception as e:
        print(f"  {COLOR_RED}❌ Ошибка: {type(e).__name__}: {e}{COLOR_RESET}")
        return False

# ================= MAIN =================

def main():
    global DEFAULT_THREADS, MAX_THREADS, HLS_THREADS, content_title, content_type
    enable_ansi_support()
    
    parser = argparse.ArgumentParser(
        description="🎬 HDRezka Downloader v4.6.5 //Уверенная База//",
        epilog="""
📋 ПРИМЕРЫ:
  🔹 Получить информацию и озвучки: python hdrezka-dl_v4.6.5.py "URL" -u email -p pass -s 1 -e 1 --get-translations
  🔹 Скачать сезоны 1-3: python hdrezka-dl_v4.6.5.py "URL" -u email -p pass -q 1080p -s "1-3" --all -t 56 --threads 20
  🔹 Расширенный поиск озвучек (до ID 3000): python hdrezka-dl_v4.6.5.py "URL" -u email -p pass --get-translations --sca-id 3000
        """
    )
    parser.add_argument("url", help="🔗 Ссылка")
    parser.add_argument("-u", "--username", required=True, help="👤 Логин")
    parser.add_argument("-p", "--password", required=True, help="🔑 Пароль")
    parser.add_argument("-q", "--quality", default="1080p", choices=["360p","480p","720p","1080p","1080p_ultra"])
    parser.add_argument("-s", "--season", default="1", help="📺 Сезон: '1', '1-3', '1,3,5' (по умолчанию: 1)")
    parser.add_argument("-e", "--episode", help="🎬 Серия: '1', '1-10', '1,3,5'")
    parser.add_argument("-t", "--translation", help="🎙 Озвучка: ID или название")
    parser.add_argument("--get-translations", action="store_true")
    parser.add_argument("--save-translations", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--hls-threads", type=int, default=HLS_THREADS)
    parser.add_argument("--delay", type=int, default=2)
    parser.add_argument("-o", "--output", default="~/Downloads/HDRezka")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-range", action="store_true")
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--max-seasons", type=int, default=20, help="Макс. сезонов для анализа (по умолчанию: 20)")
    parser.add_argument("--max-episodes", type=int, default=100, help="Макс. серий в сезоне для анализа (по умолчанию: 100)")
    # ✅ НОВЫЙ ПАРАМЕТР: ДИНАМИЧЕСКИЙ ПОИСК ОЗВУЧЕК
    parser.add_argument("--sca-id", type=int, default=DEFAULT_SCAN_ID, 
                       help=f"🔍 Диапазон сканирования озвучек (по умолчанию: {DEFAULT_SCAN_ID})")
    
    args = parser.parse_args()
    
    if args.threads:
        DEFAULT_THREADS = max(1, min(args.threads, MAX_THREADS))
    if args.hls_threads:
        HLS_THREADS = max(1, min(args.hls_threads, 16))
    
    OUTPUT_DIR = os.path.expanduser(args.output)
    headers = {
        "Referer": "https://hdrezka2vbppy.org/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    print_confident_base_banner()
    
    print(f"{COLOR_BOLD}{'='*70}{COLOR_RESET}")
    print(f"{COLOR_BOLD}🎬 HDRezka Downloader v4.6.5 //Уверенная База//{COLOR_RESET}")
    print(f"{COLOR_BOLD}{'='*70}{COLOR_RESET}")
    print("🔍 Анализирую страницу...")
    
    content_title, content_type = parse_content_info(args.url, headers)
    
    print(f"{COLOR_GREEN}✅ Название: {content_title}{COLOR_RESET}")
    type_display = '📺 Сериал' if content_type in ('series', 'shorts') else '🎬 Фильм'
    if content_type == 'shorts':
        type_display += ' (короткометражка)'
    print(f"{COLOR_GREEN}✅ Тип: {type_display}{COLOR_RESET}\n")
    
    if args.get_translations:
        print(f"{COLOR_BLUE}{'='*70}{COLOR_RESET}")
        print(f"{COLOR_BLUE}📋 ЭТАП 1: ПОЛУЧЕНИЕ СПИСКА ОЗВУЧЕК{COLOR_RESET}")
        print(f"{COLOR_BLUE}{'='*70}{COLOR_RESET}\n")
        rezka = HdRezkaApi(args.url.strip())
        rezka.login(args.username, args.password)
        
        # ✅ ИСПОЛЬЗУЕМ ДИНАМИЧЕСКИЙ ДИАПАЗОН ИЗ --sca-id
        print(f"🔍 Сканирую доступные озвучки (тихий режим, до ID {args.sca_id})...")
        translations, method = get_translations_from_page(rezka, args.url, 1, 1, scan_max_id=args.sca_id)
        
        if not translations:
            print(f"\n{COLOR_RED}❌ Не удалось получить список озвучек{COLOR_RESET}")
            sys.exit(1)
        
        print(f"\n{COLOR_GREEN}✅ Найдено {len(translations)-1} озвучек (метод: {method}){COLOR_RESET}")
        print(f"\n{COLOR_BLUE}{'='*70}{COLOR_RESET}")
        print(f"{COLOR_BOLD}📋 ДОСТУПНЫЕ ОЗВУЧКИ:{COLOR_RESET}")
        print(f"{COLOR_BLUE}{'='*70}{COLOR_RESET}")
        print(f"{'ID':<6} {'Название':<45} {'Команда':<20}")
        print("-"*70)
        for t_id, t_name in translations:
            t_id_str = str(t_id) if t_id is not None else "def"
            cmd = f"-t {t_id}" if t_id else "-t default"
            print(f"{t_id_str:<6} {COLOR_CYAN}{t_name:<45}{COLOR_RESET} {cmd:<20}")
        print(f"{COLOR_BLUE}{'='*70}{COLOR_RESET}")
        
        if content_type in ('series', 'shorts'):
            print_seasons_analysis(translations, rezka, 
                                  max_seasons=args.max_seasons, 
                                  max_episodes=args.max_episodes)
        
        print(f"\n{COLOR_YELLOW}💡 ИСПОЛЬЗУЙТЕ ЭТУ КОМАНДУ ДЛЯ СКАЧИВАНИЯ:{COLOR_RESET}")
        print(f"\n   python hdrezka-dl_v4.6.5.py \"{args.url}\" -u {args.username} -p {args.password} -q 1080p -s \"1-3\" --all -t <ID> --threads 20\n")
        
        if args.save_translations:
            save_translations_to_file(translations)
        return
    
    print("🔐 Авторизация...")
    try:
        rezka = HdRezkaApi(args.url.strip())
        rezka.login(args.username, args.password)
        print(f"{COLOR_GREEN}✅ Успешно!{COLOR_RESET}\n")
    except Exception as e:
        print(f"{COLOR_RED}❌ Ошибка авторизации: {e}{COLOR_RESET}")
        sys.exit(1)
    
    print(f"{COLOR_BLUE}{'='*70}{COLOR_RESET}")
    print(f"{COLOR_BLUE}🎬 ЭТАП 2: СКАЧИВАНИЕ{COLOR_RESET}")
    print(f"{COLOR_BLUE}{'='*70}{COLOR_RESET}\n")
    
    translation_id = None
    if args.translation:
        if args.translation.lower() in ['default', 'def', 'по умолчанию']:
            translation_id = None
            print("🎙 Озвучка: По умолчанию")
        elif args.translation.isdigit():
            translation_id = int(args.translation) if int(args.translation) > 0 else None
            print(f"🎙 Озвучка: ID {args.translation}")
        else:
            print("🔍 Поиск озвучки по названию...")
            translations, _ = get_translations_from_page(rezka, args.url, 1, 1, scan_max_id=args.sca_id)
            found = False
            for t_id, t_name in translations:
                if args.translation.lower() in t_name.lower():
                    translation_id = t_id
                    print(f"{COLOR_GREEN}✅ Найдена: {t_name} (ID: {t_id}){COLOR_RESET}")
                    found = True
                    break
            if not found:
                print(f"⚠️  Озвучка '{args.translation}' не найдена, использую По умолчанию")
    
    is_series = (content_type in ('series', 'shorts'))
    
    # ✅ ОБРАБОТКА ДИАПАЗОНОВ СЕЗОНОВ
    season_list = None
    if args.season and is_series:
        season_list = parse_range(args.season, max_value=50)
        if season_list and len(season_list) > 1:
            print(f"📦 Режим: {COLOR_GREEN}НЕСКОЛЬКО СЕЗОНОВ ({', '.join(map(str, season_list))}){COLOR_RESET}")
        elif season_list:
            print(f"📦 Режим: {COLOR_GREEN}СЕЗОН {season_list[0]}{COLOR_RESET}")
    else:
        try:
            season_list = [int(args.season)] if args.season else [1]
        except ValueError:
            season_list = [1]
    
    episodes = None
    if args.all:
        print(f"📦 Режим: {COLOR_GREEN}ВЕСЬ СЕЗОН{COLOR_RESET}")
    elif args.episode:
        episodes = parse_episodes(args.episode)
        print(f"🎬 Серии: {episodes}")
    else:
        episodes = [1]
        print(f"🎬 Серия: 1")
    
    print(f"🎬 Контент: {COLOR_BOLD}{content_title}{COLOR_RESET} ({type_display})")
    print(f"🎥 Качество: {args.quality}")
    print(f"📁 Папка: {OUTPUT_DIR}")
    print(f"{COLOR_BLUE}{'='*70}{COLOR_RESET}\n")
    
    # ✅ МНОГОСЕЗОННАЯ ЗАГРУЗКА
    total_downloaded = 0
    total_expected = 0
    start_time = time.time()
    
    for season in season_list:
        print(f"\n{COLOR_BOLD}{'='*70}{COLOR_RESET}")
        print(f"{COLOR_BOLD}📺 СЕЗОН {season}{COLOR_RESET}")
        print(f"{COLOR_BOLD}{'='*70}{COLOR_RESET}\n")
        
        current_episodes = episodes
        if args.all and current_episodes is None:
            print("🔍 Определяю количество серий...")
            total_eps = 0
            for ep in range(100, 0, -1):
                try:
                    stream = rezka.getStream(season=season, episode=ep, translation=translation_id)
                    url = stream('480p')
                    if url and extract_single_url(url):
                        total_eps = ep
                        break
                except:
                    continue
            if total_eps == 0:
                total_eps = 20
            current_episodes = list(range(1, total_eps + 1))
            print(f"{COLOR_GREEN}✅ Найдено: {len(current_episodes)} серий ({current_episodes[0]}-{current_episodes[-1]}){COLOR_RESET}\n")
        
        if args.dry_run:
            print("🔍 РЕЖИМ ПРОВЕРКИ:\n")
            for ep in current_episodes:
                print(f"  📺 S{season:02d}E{ep:02d}")
            continue
        
        print(f"{COLOR_BOLD}🚀 НАЧИНАЮ ЗАГРУЗКУ СЕЗОНА {season}:{COLOR_RESET}\n")
        season_success = 0
        
        for i, episode in enumerate(current_episodes, 1):
            print(f"\n{COLOR_BLUE}{'─'*70}{COLOR_RESET}")
            ep_display = f"Сезон {season}, Серия {episode}"
            print(f"{COLOR_BOLD}[{i}/{len(current_episodes)}] 📺 {content_title} — {ep_display}{COLOR_RESET}")
            print(f"{COLOR_BLUE}{'─'*70}{COLOR_RESET}")
            
            if not args.no_progress:
                overall_prefix = f"{COLOR_YELLOW}ПРОГРЕСС СЕЗОНА{COLOR_RESET}"
                overall_suffix = f"| Серия {i}/{len(current_episodes)}"
                print_progress_bar(i-1, len(current_episodes), prefix=overall_prefix, suffix=overall_suffix, length=60)
            
            if download_episode(rezka, args.quality, season, episode, OUTPUT_DIR, headers, 
                              translation_id, args.force_range, DEFAULT_THREADS, not args.no_progress):
                season_success += 1
                total_downloaded += 1
            
            if i < len(current_episodes):
                print(f"\n⏱ Пауза {args.delay} сек...")
                time.sleep(args.delay)
        
        total_expected += len(current_episodes)
        print(f"\n{COLOR_GREEN}✅ Сезон {season} завершён: {season_success}/{len(current_episodes)} серий{COLOR_RESET}")
    
    total_time = time.time() - start_time
    if not args.no_progress:
        print()
        print_progress_bar(total_expected, total_expected, prefix=f"{COLOR_GREEN}✅ ВСЕГО ЗАВЕРШЕНО{COLOR_RESET}", 
                          suffix=f"| {total_downloaded}/{total_expected}", length=60)
    
    print(f"\n\n{COLOR_BOLD}{'='*70}{COLOR_RESET}")
    print(f"{COLOR_BOLD}📊 ИТОГИ:{COLOR_RESET}")
    print(f"   ✅ Скачано: {COLOR_GREEN}{total_downloaded}/{total_expected}{COLOR_RESET} серий")
    print(f"   📺 Сезонов: {len(season_list)}")
    print(f"   ⏱  Время: {COLOR_YELLOW}{format_time(total_time)}{COLOR_RESET}")
    print(f"   📁 Папка: {COLOR_BLUE}{OUTPUT_DIR}{COLOR_RESET}")
    if total_downloaded == total_expected:
        print(f"   {COLOR_GREEN}🎉 ВСЁ ГОТОВО! Приятного просмотра! ✨{COLOR_RESET}")
    elif total_downloaded > 0:
        print(f"   {COLOR_YELLOW}⚠️  Часть не скачалась{COLOR_RESET}")
    else:
        print(f"   {COLOR_RED}❌ Ни одна не скачалась{COLOR_RESET}")
    print(f"{COLOR_BOLD}{'='*70}{COLOR_RESET}\n")

if __name__ == "__main__":
    main()
