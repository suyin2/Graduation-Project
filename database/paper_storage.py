from pathlib import Path
import requests


class PaperStorage:
    def __init__(self, storage_dir: str = "storage/papers"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def get_arxiv_pdf_url(self, link: str) -> str:
        if not link:
            return ""
        if "/abs/" in link:
            paper_id = link.split("/abs/")[-1].strip()
            return f"https://arxiv.org/pdf/{paper_id}.pdf"
        if "/pdf/" in link:
            return link
        return ""

    def extract_paper_id(self, link: str) -> str:
        if not link:
            return ""
        if "/abs/" in link:
            return link.split("/abs/")[-1].strip()
        if "/pdf/" in link:
            return link.split("/pdf/")[-1].replace(".pdf", "").strip()
        return link.strip()

    def build_pdf_path(self, paper_id: str) -> Path:
        safe_id = paper_id.replace("/", "_")
        return self.storage_dir / f"{safe_id}.pdf"

    def download_pdf(self, pdf_url: str, save_path: Path) -> str:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(pdf_url, headers=headers, timeout=30)
        resp.raise_for_status()

        with open(save_path, "wb") as f:
            f.write(resp.content)

        return str(save_path)

    def save_paper_pdf(self, paper: dict) -> dict:
        link = paper.get("link", "")
        paper_id = self.extract_paper_id(link)
        pdf_url = self.get_arxiv_pdf_url(link)

        if not paper_id or not pdf_url:
            return {
                "paper_id": paper_id,
                "pdf_url": pdf_url,
                "pdf_local_path": ""
            }

        pdf_path = self.build_pdf_path(paper_id)

        if not pdf_path.exists():
            self.download_pdf(pdf_url, pdf_path)

        return {
            "paper_id": paper_id,
            "pdf_url": pdf_url,
            "pdf_local_path": str(pdf_path)
        }