package test.image;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.molgenis.util.ImageMagickResizer;

public class ResizeImageTest
{
	static Logger logger = Logger.getLogger(ResizeImageTest.class);

	public static void main(String[] args) throws Exception
	{

		// generate thumbnails for all files in test folder
		File imageDir = new File("image");
		File thumbsDir = new File("thumbs");

		//resizeDir(imageDir, thumbsDir, 24, 24);
		// resizeDir(imageDir, thumbsDir, 64,64);
		resizeDir(imageDir, thumbsDir, 128, 128);

	}

	/**
	 * http://code.google.com/p/java-image-scaling/
	 * 
	 * @param imageDir
	 * @param thumbsDir
	 * @param targetWidth
	 * @param targetHeight
	 * @throws Exception 
	 */
	public static void resizeDir(File imageDir, File thumbsDir, int width, int height) throws Exception
	{
		logger.debug("resizing images");

		//Image srcImage = null;
		//BufferedImage rescaledImage = null;
		for (String imageName : imageDir.list())
		{
			File src = new File(imageDir.getAbsoluteFile() + File.separator + imageName);
			File dest = new File(thumbsDir.getAbsolutePath() + File.separator + width + "x" + height + "_" + imageName);

			new ImageMagickResizer().resize(src, dest, width, height);
			//BufferedImageResizer.resize(src,dest,width,height);

			// write result
			// ImageIO.write(rescaledImage, "png", dest);
			// ImageResizer.resize(src, out, width, height);

			logger.debug("resizing image " + src);
			logger.debug("resized image to " + dest);

		}
	}	
}
