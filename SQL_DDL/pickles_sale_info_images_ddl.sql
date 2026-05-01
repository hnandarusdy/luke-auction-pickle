-- DDL for pickles_sale_info_images table
-- This table stores flattened image data from auction JSON files

CREATE TABLE `pickles_sale_info_images` (
    `auto_id` INT AUTO_INCREMENT PRIMARY KEY,
    `id` VARCHAR(50) NOT NULL COMMENT 'Product ID from JSON',
    `lotNumber` VARCHAR(50) NULL COMMENT 'Lot number',
    `productId` VARCHAR(255) NULL COMMENT 'UUID product identifier',
    `productLine` VARCHAR(255) NULL COMMENT 'Product line category',
    `stockNumber` VARCHAR(50) NULL COMMENT 'Stock number',
    `title` TEXT NULL COMMENT 'Product title',
    `saleId` INT NULL COMMENT 'Sale ID',
    `saleNumber` INT NULL COMMENT 'Sale number',
    `imageId` BIGINT NULL COMMENT 'Image ID from Pickles CDN',
    `cdnUrl` TEXT NULL COMMENT 'CDN URL for the image',
    `sequence` INT NULL COMMENT 'Image sequence order',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Record creation time',
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Record last update time'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Flattened image data from auction JSON files';