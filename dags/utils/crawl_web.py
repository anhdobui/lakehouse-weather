import os
import re
import json
from langchain_community.document_loaders import RecursiveUrlLoader
from langchain_community.document_loaders import UnstructuredPDFLoader
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests
import urllib.parse
import tldextract

load_dotenv()


def bs4_extractor(html: str) -> str:
    """
    Hàm trích xuất và làm sạch nội dung từ HTML.
    Args:
        html: Chuỗi HTML cần xử lý.
    Returns:
        str: Văn bản đã được làm sạch, loại bỏ các thẻ HTML và khoảng trắng thừa.
    """
    soup = BeautifulSoup(html, "html.parser")
    return re.sub(r"\n\n+", "\n\n", soup.text).strip()  # Xóa khoảng trắng và dòng trống thừa


def crawl_pdf(url: str) -> dict:
    """
    Tải và xử lý PDF từ URL.
    Args:
        url (str): Đường dẫn đến file PDF.
    Returns:
        dict: Nội dung và metadata của file PDF.
    """
    try:
        # Tải file PDF
        response = requests.get(url)
        pdf_path = "/tmp/temp.pdf"
        with open(pdf_path, "wb") as f:
            f.write(response.content)
        print(f"Extracted from {url}")
        # Sử dụng UnstructuredPDFLoader để xử lý PDF
        loader = UnstructuredPDFLoader(pdf_path)
        documents = loader.load()
        os.remove(pdf_path)  # Xóa file tạm

        # Trả về dữ liệu đã xử lý
        return [{"page_content": doc.page_content, "metadata": doc.metadata} for doc in documents]
    except Exception as e:
        print(f"Error processing PDF {url}: {e}")
        return []

def crawl_html(url: str) -> dict:
    """
    Trích xuất nội dung và danh sách liên kết từ một trang HTML.
    Args:
        url (str): URL của trang HTML.
    Returns:
        dict: Bao gồm nội dung trang và danh sách các liên kết.
    """
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Trích xuất nội dung văn bản từ HTML
        content = bs4_extractor(response.text)
        metadata = {"source": url, "content_type": "text/html"}

        # Tìm tất cả liên kết trong trang
        soup = BeautifulSoup(response.text, "html.parser")
        links = [
            urllib.parse.urljoin(url, a['href'])
            for a in soup.find_all("a", href=True)
            if a['href'] and not a['href'].startswith(("mailto:", "tel:", "#"))
        ]

        print(f"Extracted {len(links)} links from {url}")

        return {"page_content": content, "metadata": metadata, "links": links}

    except Exception as e:
        print(f"Error processing HTML {url}: {e}")
        return {"page_content": "", "metadata": {}, "links": []}

def crawl_web(url: str, depth: int = 1, visited_urls: set = None) -> list:
    """
    Quét URL với khả năng đệ quy.
    Args:
        url (str): URL cần quét.
        depth (int): Độ sâu tối đa.
        visited_urls (set): Tập hợp các URL đã quét.
    Returns:
        list: Danh sách dữ liệu đã quét.
    """
    if visited_urls is None:
        visited_urls = set()

    # Nếu độ sâu bằng 0 hoặc URL đã được quét, dừng lại
    if depth == 0 or url in visited_urls:
        return []

    visited_urls.add(url)
    print(f"Crawling URL: {url} at depth {depth}")

    try:
        # Kiểm tra đuôi file trước
        if url.lower().endswith(".pdf"):
            print(f"Detected PDF content at {url}")
            return crawl_pdf(url)             
        # Kiểm tra loại nội dung của URL
        response = requests.head(url, allow_redirects=True)
        content_type = response.headers.get("Content-Type", "").lower()

        if "application/pdf" in content_type:
            print(f"Detected PDF content: {url}")
            return crawl_pdf(url)  
        elif "text/html" in content_type:
            print(f"Detected HTML content: {url}")
            html_data = crawl_html(url)  # Trích xuất nội dung HTML

            if depth > 1:
                all_data = [html_data]
                for link in html_data["links"]:
                    if link not in visited_urls:
                        try:
                            # Kiểm tra Content-Type của liên kết
                            head_response = requests.head(link, allow_redirects=True, timeout=10)
                            link_content_type = head_response.headers.get("Content-Type", "").lower()

                            if "application/pdf" in link_content_type:
                                print(f"Detected PDF content at {link}")
                                pdf_data = crawl_pdf(link)
                                all_data.extend(pdf_data)
                            elif "text/html" in link_content_type:
                                all_data.extend(crawl_web(link, depth=depth - 1, visited_urls=visited_urls))
                        except Exception as e:
                            print(f"Error processing link {link}: {e}")

                return all_data
            else:
                return [html_data]
        else:
            print(f"Unsupported content type: {url}")
            return []

    except Exception as e:
        print(f"Error crawling URL {url}: {e}")
        return []
if __name__ == "__main__":
    url_list = ["https://pola.rs/"]
    depth = 2  # Quét sâu tối đa 2 cấp liên kết
    data = crawl_web(url_list, depth=depth)
    print(json.dumps(data, ensure_ascii=False, indent=4))
