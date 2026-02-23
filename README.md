🗂️ AI Image Segregator in Database

Auto-Bucket Smart Image Sorting for Cloudflare R2 + Local Folders

AI Image Segregator is an intelligent image classification and smart-sorting tool built with Streamlit, Pillow, and Cloudflare R2 (S3-compatible).

It automatically:

📂 Classifies images (Nature, Food, People, etc.)

🔁 Groups similar images using perceptual hashing + pixel similarity

🚫 Detects placeholder images (e.g., “No Image Available”)

☁️ Works directly with Cloudflare R2 buckets

📁 Also works with local folders

📋 Generates structured JSON logs for every operation

🖼️ Provides a visual gallery with preview before segregation

🚀 Features
1️⃣ Automatic AI Classification

Each image is analyzed using:

HSV color distribution

Edge detection

Aspect ratio

Texture complexity

Heuristic category rules

Images are grouped into:

Nature/
Food/
People/
Architecture/
Vehicles/
Technology/
Art/
Sports/
Abstract/
2️⃣ Smart Reference-Based Sorting

Upload one reference image and:

Finds visually similar images

Uses:

Perceptual hash comparison

Hamming distance

Pixel similarity

Moves only matched images

Keeps others unchanged

Supports preview JSON before execution

3️⃣ Placeholder Image Detection

Detects images like:

"No Image Available"

"Coming Soon"

"No Floor Plan"

Blank white background with text

Uses:

Background color dominance

Edge score analysis

OCR (if pytesseract installed)

Pixel similarity rules

Automatically moves them to:

no_image/
4️⃣ Cloudflare R2 Integration

Works directly with R2 buckets.

Auto Features:

Bucket auto-detection

Folder scanner

Quick scan mode

Presigned URL preview

Safe file renaming

Batch delete options

5️⃣ Local Folder Support

Also supports:

Local full-folder classification

Local reference-based sorting

Local copy instead of delete

Preview before segregation

6️⃣ JSON Session Logging

Every operation generates a structured JSON log:

{
  "event_type": "r2_folder_upload",
  "bucket": "img",
  "source_prefix": "images/masterImgs/",
  "output_prefix": "images/masterImgs/nature_forest/",
  "results": {
    "moved_count": 25,
    "unchanged_count": 0
  },
  "extra": {
    "moved": 25,
    "unchanged_count": 78,
    "total_processed": 103
  }
}

Log file:

segregator_session_log.json

Downloadable from sidebar.

🏗️ Architecture
Streamlit UI
        ↓
Image Analysis Engine (Pillow + Heuristics)
        ↓
Similarity Engine (pHash + Pixel Compare)
        ↓
Cloudflare R2 (boto3 S3 client)
        ↓
JSON Logger
🛠️ Tech Stack

Python 3.9+

Streamlit

Pillow

boto3

Cloudflare R2 (S3 API)

pytesseract (optional)

JSON logging

📦 Installation
1️⃣ Clone the Project
git clone <your-repo-url>
cd ai-image-segregator
2️⃣ Install Dependencies
pip install streamlit boto3 pillow botocore pytesseract

If using OCR:

Install Tesseract engine:

Windows:

Download from:
https://github.com/tesseract-ocr/tesseract

Linux:

sudo apt install tesseract-ocr
3️⃣ Configure R2 Credentials

Inside app.py:

R2_ENDPOINT   = "YOUR_ENDPOINT"
R2_ACCESS_KEY = "YOUR_ACCESS_KEY"
R2_SECRET_KEY = "YOUR_SECRET_KEY"

⚠️ Recommended: Move credentials to environment variables for production.

4️⃣ Run the App
streamlit run app.py
🖥️ How to Use
Step 1 — Select Bucket

Refresh bucket list

Select your R2 bucket

Step 2 — Use Bucket Scanner

Click ⚡ Quick Scan

Copy folder path

Paste into "Source Prefix"

Step 3 — Choose Mode
☁️ R2 Full Bucket

Classify entire folder automatically.

☁️ R2 By Reference

Upload reference image → find similar.

📁 Local Full Folder

Sort local images.

🔍 Local By Reference

Reference-based sorting for local folder.

🖼️ Gallery Mode

Browse folders visually:

View thumbnails

Upload reference per folder

Preview JSON

Confirm segregation

Download segregation report

Delete selected images safely

📊 Classification Logic

Image features used:

Feature	Purpose
HSV Hue	Detect dominant color
Saturation	Detect colorful vs muted
Brightness	Detect dark/light
Edge Score	Complexity detection
Aspect Ratio	Portrait/Landscape
Pixel Similarity	Visual comparison
Perceptual Hash	Fast similarity detection
🔐 Production Recommendations

For secure deployment:

Move R2 credentials to .env

Use IAM limited access keys

Enable CORS rules in R2

Deploy on:

Streamlit Cloud

Render

AWS EC2

Azure VM

📂 Project Structure
app.py
segregator_session_log.json
README.md
🧠 Advanced Capabilities

Smart threshold tuning

Direct folder-only scan

Preview → Confirm architecture

Bulk deletion safety

Duplicate-safe renaming

Automatic subfolder detection

⚡ Performance Notes

Uses pagination for large buckets

Efficient image resizing before hashing

Temporary directory cleanup

Adaptive retry mode in boto3

🚧 Future Improvements

Deep Learning Model (ResNet / CLIP)

Face detection

Duplicate image detection

Auto tagging with embeddings

Vector database integration

Async processing

Background job queue

🧑‍💻 Author

AI Image Segregator
Built with ❤️ using Python & Streamlit

📄 License

MIT License
