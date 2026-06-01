import express from 'express';
import cors from 'cors';
import multer from 'multer';
import sharp from 'sharp';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const uploadsDir = path.join(__dirname, '../uploads');

// Ensure uploads directory exists
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

const app = express();
app.use(cors({ origin: true, credentials: true }));

// Serve the uploads directory statically
app.use('/uploads', express.static(uploadsDir));

// Multer config (store in memory temporarily to process with sharp)
const storage = multer.memoryStorage();
const upload = multer({ storage, limits: { fileSize: 10 * 1024 * 1024 } }); // 10MB limit

app.post('/api/assets/upload', upload.single('image'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No image uploaded' });
    }

    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
    const baseFilename = `prod-${uniqueSuffix}`;
    const ext = '.webp'; // Standardize format for compression

    const fullFilename = `${baseFilename}${ext}`;
    const thumbFilename = `${baseFilename}-thumb${ext}`;

    const fullPath = path.join(uploadsDir, fullFilename);
    const thumbPath = path.join(uploadsDir, thumbFilename);

    // 1. Process full image (compress and convert to WebP)
    await sharp(req.file.buffer)
      .webp({ quality: 80 })
      .toFile(fullPath);

    // 2. Process thumbnail (resize, compress, convert)
    await sharp(req.file.buffer)
      .resize(200, 200, { fit: 'cover' })
      .webp({ quality: 60 })
      .toFile(thumbPath);

    // Return the URLs
    const baseUrl = 'http://localhost:3002'; // Assets service port
    res.json({
      image: `${baseUrl}/uploads/${fullFilename}`,
      thumbnail: `${baseUrl}/uploads/${thumbFilename}`
    });

  } catch (err) {
    console.error('Error processing image:', err);
    res.status(500).json({ error: 'Image processing failed' });
  }
});

const PORT = 3002;
app.listen(PORT, () => {
  console.log(`🖼️  Assets Service running on http://localhost:${PORT}`);
});
