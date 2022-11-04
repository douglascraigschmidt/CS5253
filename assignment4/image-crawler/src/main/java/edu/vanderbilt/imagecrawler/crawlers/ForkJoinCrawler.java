package edu.vanderbilt.imagecrawler.crawlers;

import static java.util.stream.Collectors.toList;
import static edu.vanderbilt.imagecrawler.crawlers.Crawler.Type.IMAGE;
import static edu.vanderbilt.imagecrawler.crawlers.Crawler.Type.PAGE;

import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import edu.vanderbilt.imagecrawler.platform.Cache;
import edu.vanderbilt.imagecrawler.transforms.Transform;
import edu.vanderbilt.imagecrawler.utils.BlockingTask;
import edu.vanderbilt.imagecrawler.utils.ExceptionUtils;
import edu.vanderbilt.imagecrawler.utils.Image;
import edu.vanderbilt.imagecrawler.web.WebPageElement;

/**
 * This ImageCrawler implementation uses the Java common fork-join
 * pool framework and the Java sequential streams framework to perform
 * an "image crawl" starting from a root Uri.  Images from an HTML
 * page reachable from the root Uri are downloaded from a remote web
 * server or the local file system and the results are stored in files
 * on the Android device, where they can be displayed to the user.
 */
public class ForkJoinCrawler
       extends ImageCrawler {
    /**
     * Perform the web crawl.
     *
     * @param pageUri The URL that we're crawling at this point
     * @param depth   The current depth of the recursive processing
     * @return The number of images downloaded/transformed/stored
     */
    @Override
    protected int performCrawl(String pageUri, int depth) {
        // Use the Java common fork-join pool to return the result of
        // invoking the computations in an instance of the
        // URLCrawlerTask.
        // 
        // TODO -- replace return 0 with the appropriate code below.
        return 0;
    }

    /**
     * This factory method create a new {@link PerformTransformTask}.
     *
     * @param image     The {@link Image} to transform
     * @param transform The {@link Transform} to perform
     * @return A new instance of {@link PerformTransformTask} returned as
     *         as {@link ForkJoinTask}
     */
    protected ForkJoinTask<Image> makePerformTransformTask(Image image,
                                                           Transform transform) {
        // TODO -- you fill in here replacing this statement with your
        // solution.
        return null;
    }

    /**
     * This factory method create a new {@link ProcessImageTask}.
     *
     * @param url The URL to the image to process
     * @return A new {@link ProcessImageTask} that will transform the image
     */
    protected ForkJoinTask<Integer> makeProcessImageTask(String url) {
        // Create a Function that returns a new URL object when
        // applied and which converts checked URL exceptions into
        // runtime exceptions.
        Function<String, URL> urlFactory = ExceptionUtils
            .rethrowFunction(URL::new);

        // TODO -- you fill in here replacing this statement with your
        // solution.
        return null;
    }

    /**
     * This factory method create a new {@link URLCrawlerTask}.
     *
     * @param pageUri The URL to the page to crawl
     * @param depth   The maximum crawl depth
     * @return A new {@link URLCrawlerTask} instance
     */
    protected ForkJoinTask<Integer> makeURLCrawlerTask(String pageUri, int depth) {
        // TODO -- you fill in here replacing this statement with your
        // solution.
        return null;
    }

    /**
     * Perform a web crawl from a particular starting point.  By
     * extending {@link RecursiveTask}, instances of this class can be
     * forked/joined in parallel via the Java common fork-join pool.
     */
    public class URLCrawlerTask
        extends RecursiveTask<Integer> {
        /**
         * The URI that's being crawled at this point.
         */
        public String mPageUri;

        /**
         * The current depth of the recursive processing.
         */
        public int mDepth;

        /**
         * Constructor initializes the fields.
         */
        URLCrawlerTask(String pageUri, int depth) {
            mPageUri = pageUri;
            mDepth = depth;
        }

        /**
         * Perform a web crawl at the URL passed to the constructor.
         *
         * @return The number of images downloaded/transformed/stored
         *         starting at the URL passed to the constructor
         */
        @Override
        protected Integer compute() {
            log(">> Depth: " + mDepth + " [" + mPageUri + "]");
            // Create and use a Java sequential stream to perform
            // the following steps:
            //
            // 1. Use a factory method to create a one-element stream
            //    containing just the pageUri.
            //
            // 2. Use an intermediate operation to filter out pageUri
            //    if it exceeds max depth or was already visited.
            //
            // 3. Use an intermediate operation to recursively crawl
            //    all images and hyperlinks on this page and return
            //    the total number of processed images.
            //
            // 4. Use a terminal operation to get the total number of
            //    processed images from the one-element stream.

            // TODO -- you fill in here replacing this statement with
            // your solution.
            return 0;
        }

        /**
         * Perform a crawl starting at {@code pageUri} and return the
         * sum of all images processed during the crawl.
         *
         * @param pageUri The URL of the page to crawl
         * @param depth   The current depth of the recursive processing
         * @return The number of images processed during the crawl
         */
        protected int crawlPage(String pageUri, int depth) {
            // Create and use a Java sequential stream to perform the
            // following steps:
            //
            // 1. Use a factory method to create a one-element stream
            //    containing just the pageUri.
            //
            // 2. Get the HTML page associated with pageUri using the
            //    ImageCrawler.callInManagedBlocker() method to
            //    expand the Java common fork-join pool.
            //
            // 3. Filter out a missing (null) HTML page.
            //
            // 4. Call processPage() to process images encountered
            //    during the crawl.
            //
            // 5. Use a terminal operation to get the total number of
            //    processed images from the one-element stream.

            // TODO -- you fill in here replacing return 0 with
            // your solution.
            return 0;
        }

        /**
         * Use a Java sequential stream and the fork-join framework to
         * (1) download and process images on this page via a
         * ProcessImageTask object, (2) recursively crawl other
         * hyperlinks accessible from this page via a URLCrawlerTask
         * object, and (3) return the count of all images processed
         * during the crawl.
         *
         * @param page The {@link Crawler.Page} containing the HTML
         * @param depth The current depth of the recursive processing
         * @return The count of the number of images processed
         */
        protected int processPage(Crawler.Page page,
                                  int depth) {
            // Create and use a Java sequential stream to perform the
            // following steps:
            //
            // 1. Get a List containing all the image/page elements on
            //    this page.
            //
            // 2. Convert the List to a sequential stream.
            //
            // 3. Map each web element to a count of images produced
            //    by either (1) download and process images on this
            //    page via a ProcessImageTask object or (2)
            //    recursively crawling other hyperlinks accessible
            //    from this page via a URLCrawlerTask object.
            //
            // 4. Sum all the results together.

            // TODO -- you fill in here replacing this statement
            // with your solution.
            List<ForkJoinTask<Integer>> forks = null;

            // Call a method that joins all the forked tasks and
            // returns a sum of the number of images returned from
            // each task.
            // TODO -- you fill in here replacing return 0 with your
            // solution.
            return 0;
        }

        /**
         * Join all the {@link ForkJoinTask} objects and return a sum
         * of the number of images returned from each task.
         *
         * @param forks A {@link List} of {@link ForkJoinTask}
         *              objects containing number of {@link Integer}
         *              objects
         * @return The sum of the number of images processed
         */
        protected int sumResults(List<ForkJoinTask<Integer>> forks) {
            // Create and use a Java sequential stream to perform the
            // following steps:
            //
            // 1. Convert the forks array into a sequential stream.
            //
            // 2. Use an intermediate operation to join the tasks,
            //    where join() returns the # of images processes.
            //
            // 3. Sum all the # of images processed.

            // TODO -- you fill in here replacing this statement with
            // your solution.
            return 0;
        }
    }

    /**
     * Download and process an image.  By extending {@link
     * RecursiveTask}, instances of this can be forked/joined in
     * parallel by the Java common fork-join pool.
     */
    public class ProcessImageTask
        extends RecursiveTask<Integer> {
        /**
         * A URL to the image to process.
         */
        final URL mImageUri;

        /**
         * Constructor initializes the fields.
         *
         * @param imageUri The URL to process
         */
        ProcessImageTask(URL imageUri) {
            mImageUri = imageUri;
        }

        /**
         * Download and process an image.
         *
         * @return A count of the number of images processed
         */
        @Override
        protected Integer compute() {
            // Create and use a Java sequential stream to perform
            // the following steps:
            //
            // 1. Use a factory method to create a one-element stream
            //    containing just the image URI.
            //
            // 2. Get or download the image from the given url using
            //    the managedBlockerDownloadImage() method.
            //
            // 3. Filter out a missing (null) image.
            //
            // 4. Transform the image and return a count of the number
            //    of transforms applied.
            //
            // 5. Use a terminal operation to get the total number of
            //    processed images from the one-element stream.

            // TODO -- you fill in here replacing return null with your
            // solution.
            return null;
        }

        /**
         * Apply the current set of crawler transforms on the passed
         * {@link Image} and returns a count of all successfully
         * transformed images.
         *
         * @param image The {@link Image} to transform locally
         * @return The count of the non-null transformed images
         */
        protected int transformImage(Image image) {
            // Create and use a Java sequential stream to perform the
            // following steps:
            // 
            // 1. Convert mTransforms List to a stream.
            //
            // 2. Create and fork a PerformTransformTask to transform
            //    this image.
            //
            // 3. Trigger intermediate operation processing and
            //    collect results into an array.
            // TODO -- you fill in here replacing this statement
            //  with your solution.
            List<ForkJoinTask<Image>> forks = null;

            // Call a method that joins all the forked tasks and counts
            // the number of non-null images returned from each task.
            // TODO -- you fill in here replacing return 0 with your
            // solution.
            return 0;
        }

        /**
         * Join all the forked tasks and count the number of non-null
         * images returned from each task.
         *
         * @param forks A {@link List} of {@link ForkJoinTask}
         *              objects containing number of {@link Image}
         *              objects
         * @return A count of the number of non-null images transformed
         */
        protected int countTransformations(List<ForkJoinTask<Image>> forks) {
            // Create and use a Java sequential stream to perform the
            // following steps:
            // 
            // 1. Convert the forks array into a sequential stream.
            //
            // 2. Use an intermediate operation to join the tasks and
            //    filter out unsuccessful transform operations.
            //
            // 3. Count the number of successful transforms.

            // TODO -- you fill in here replacing this statement with
            // your solution.
            return 0;
        }
    }

    /**
     * Perform transform operations.  By extending {@link
     * RecursiveTask}, instances of this class can be forked/joined in
     * parallel by the Java common fork-join pool.
     */
    public class PerformTransformTask
        extends RecursiveTask<Image> {
        /**
         * Image to process.
         */
        final Image mImage;

        /**
         * Transform to apply.
         */
        final Transform mTransform;

        /**
         * Constructor initializes the fields.
         *
         * @param image     An {@link Image} that's been downloaded
         * @param transform The {@link Transform} to perform
         */
        PerformTransformTask(Image image, Transform transform) {
            mImage = image;
            mTransform = transform;
        }

        /**
         * Transform and store an {@link Image}.
         *
         * @return A transformed and stored {@link Image}
         */
        @Override
        protected Image compute() {
            // Create and use a Java sequential stream to perform
            // the following steps:
            //
            // 1. Use a factory method to create a one-element stream
            //    containing just the transform to apply.
            //
            // 2. Attempt to create a new cache item for each image,
            //    filtering out any image that has already been locally
            //    cached.
            //
            // 3. Use an intermediate operation to apply the
            //    transform to the image.
            //
            // 4. Filter out a null image that wasn't transformed.
            //
            // 5. Use a terminal operation to get the transformed
            //    image from the one-element stream or else return
            //    null.

            // TODO -- you fill in here replacing this statement with
            // your solution.
            return null;
        }
    }

    /**
     * Use {@link BlockingTask} to encapsulate the {@link Supplier} so
     * it runs in the context of the Java common fork-join pool {@link
     * ForkJoinPool.ManagedBlocker} mechanism.
     *
     * @param supplier The {@link Supplier} to call
     * @return The result of calling the {@link Supplier} in the
     * context of the Java common fork-join pool {@link
     * ForkJoinPool.ManagedBlocker} mechanism
     */
    @Override
    protected <T> T callInManagedBlocker(Supplier<T> supplier) {
        // Use BlockingTask.callInManagedBlock() to run the supplier
        // as a ManagedBlocker.
        // TODO -- you fill in here replacing null with your solution.
        return null;
    }

    /**
     * Convert {@link Cache.Item} to an Image by downloading it.
     * This call ensures the common fork/join thread pool is expanded
     * to handle the blocking image download.
     *
     * @param item The {@link Cache.Item} to download
     * @return The downloaded {@link Image}
     */
    @Override
    protected Image managedBlockerDownloadImage(Cache.Item item) {
        // Use callInManagedBlocker() and downloadImage() to download
        // the item in a ManagedBlocker.
        // TODO -- you fill in here replacing null with your solution.
        return null;
    }
}
