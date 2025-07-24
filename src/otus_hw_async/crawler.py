"""Кроулер сайта news.ycombinator.com."""

# Асинхроность
import asyncio
import aiohttp
import aiofiles
import aiofiles.os

# Работа с каталогами
from pathlib import Path
import shutil
from hashlib import sha256

# Парсинг сайта
from bs4 import BeautifulSoup, Tag
import mimetypes

# Журналирование
import logging
import sys

from typing import Tuple, Generator


class Crawler:
    """Кроулер сайта news.ycombinator.com."""

    def __init__(self):
        """Инициализация."""
        # URL корневого узла
        self.top_url = "http://news.ycombinator.com"
        # Задержка перед повторным обращением к сайту
        self.delay = 5
        # Количество новостей, попадающих в топ
        self.news_limit = 30
        # Идентификаторы найденных новостей из топа
        self.recent_news = set()
        # Каталог для сохранения новостей (каталог data в корне проекта)
        self.current_directory = Path(__file__).parent.parent.parent.joinpath(
            "data")

    @staticmethod
    def get_file_extension_from_response(response) -> str:
        """Определение расширения по MIME-типу.

        :arg response - ответ HTTP-сервера
        :return: Расширение сохраняемого файла
        """
        content_type = response.headers.get("Content-Type", "text/html")
        content_type = content_type.split(";")[0]
        ext = mimetypes.guess_extension(content_type)
        return ext

    def prepare_directory(self):
        """Очистка данных предыдущего запуска."""
        if self.current_directory.exists():
            shutil.rmtree(self.current_directory)
        self.current_directory.mkdir(parents=True, exist_ok=True)

    def init_logger(self):
        """Инициализация журнала."""
        logging.basicConfig(
            level=logging.INFO,
            filename=self.current_directory.joinpath("log.txt"),
            filemode="w",
        )

    def find_link(self, articles: Tag) -> Tuple[str, str]:
        """Находим название статьи и ссылку на нее.

        :arg articles - Распарсенный html-код
        :return: Название статьи, ссылка
        """
        for span in articles.find_all("span", attrs={"class": "titleline"}):
            for link in span.find_all("a"):
                href = link["href"]
                if href.startswith("item"):
                    href = self.top_url + "/" + href
                return link.string, href
        return "", ""

    def get_news(self, html) -> Generator[Tuple[int, str, str], None, None]:
        """Находим новость.

        :arg html - html-код
        :return: ID новости, название статьи, ссылка
        """
        soup = BeautifulSoup(html, "lxml")
        news_count = 0
        for articles in soup.find_all("tr", attrs={"class": ["athing", "submission"]}):
            news_count += 1
            if news_count > self.news_limit:
                raise StopIteration
            news_id = int(articles["id"])
            # Старые новости не нужны
            if news_id not in self.recent_news:
                self.recent_news.add(news_id)
                descr, href = self.find_link(articles)
                yield news_id, descr, href

    @staticmethod
    async def url_to_file(url, path, default_name=""):
        """Сохранение файла по URL в файл.

        :arg url - url-адрес
        :arg path - путь к папке
        """
        if default_name != "":
            filename = default_name
        else:
            url_hash = sha256(url.encode()).hexdigest()[:50]
            filename = f"{url_hash}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        ext = Crawler.get_file_extension_from_response(response)
                        if ext is not None:
                            filename += ext
                        full_path = path.joinpath(filename)
                        async with aiofiles.open(full_path, "wb") as f:
                            await f.write(await response.read())
                        if default_name == "":
                            logging.info("COMMENT URL:" + url)
                            logging.info("COMMENT PATH:" + str(full_path))
        except (
            aiohttp.client_exceptions.ClientConnectorError,
            aiohttp.client_exceptions.ConnectionTimeoutError,
            aiohttp.client_exceptions.ClientOSError,
            aiohttp.client_exceptions.ClientPayloadError,
            asyncio.TimeoutError,
        ) as e:
            logging.error(e)

    async def parse_comments_page(self, html, path):
        """Парсинг страницы с комментариями.

        :arg html - код страницы
        :arg path - путь к каталогу с файлами
        """
        soup = BeautifulSoup(html, "lxml")
        async with asyncio.TaskGroup() as tg:
            for comment in soup.find_all("div", attrs={"class": ["commtext", "c00"]}):
                for link in comment.find_all("a"):
                    href = link["href"]
                    if href.startswith("item"):
                        href = self.top_url + "/" + href
                    tg.create_task(self.url_to_file(href, path))

    async def load_comments(self, news_id, path):
        """Загрузка страницы с комментариями.

        :arg news_id - ID новости
        :arg path - путь к каталогу с файлами
        """
        url = self.top_url + "/item?id=" + str(news_id)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        return
                    html = await response.text()
                    # Извлекаем ссылки из комментариев
                    await self.parse_comments_page(html, path)
        except (
            aiohttp.client_exceptions.ClientConnectorError,
            aiohttp.client_exceptions.ConnectionTimeoutError,
            aiohttp.client_exceptions.ClientOSError,
            aiohttp.client_exceptions.ClientPayloadError,
            asyncio.TimeoutError,
        ) as e:
            logging.error(e)

    async def parse_index_page(self, html):
        """Парсинг главной страницы.

        :arg html - код страницы
        """
        async with asyncio.TaskGroup() as tg:
            for news_id, news_text, news_href in self.get_news(html):
                news_path = self.current_directory.joinpath(str(news_id))
                # Каталог для сохранения
                await aiofiles.os.mkdir(news_path)
                logging.info("NEWS ID:" + str(news_id))
                logging.info("NEWS TEXT:" + news_text)
                logging.info("NEWS URL:" + news_href)
                tg.create_task(self.load_comments(news_id, news_path))
                tg.create_task(self.url_to_file(news_href, news_path, "index"))

    async def iteration(self):
        """Однократный проход по сайту."""
        html = ""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.top_url) as response:
                    if response.status != 200:
                        return
                    html = await response.text()
            # Извлекаем новости
            await self.parse_index_page(html)
        # await process_news(session, page_content)
        except (
            aiohttp.client_exceptions.ClientConnectorError,
            aiohttp.client_exceptions.ConnectionTimeoutError,
            aiohttp.client_exceptions.ClientOSError,
            aiohttp.client_exceptions.ClientPayloadError,
            asyncio.TimeoutError,
        ) as e:
            logging.error(e)
            return

    async def crawl(self):
        """Многократный проход по сайту."""
        self.prepare_directory()
        self.init_logger()
        iteration = 0
        while True:
            iteration += 1
            logging.info("Iteration #" + str(iteration))
            await self.iteration()
            await asyncio.sleep(self.delay)


async def main():
    """Запуск краулера."""
    crawler = Crawler()
    await crawler.crawl()


if __name__ == "__main__":
    print("Crawler started")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Closing crawler")
        sys.exit(0)
    except BaseException as be:
        logging.error(be)
        sys.exit(1)
