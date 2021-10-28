package edu.vanderbilt.imagecrawler.crawlers

import admin.AssignmentTests
import edu.vanderbilt.imagecrawler.transforms.Transform
import edu.vanderbilt.imagecrawler.utils.*
import edu.vanderbilt.imagecrawler.utils.Crawler.Type.IMAGE
import edu.vanderbilt.imagecrawler.utils.Crawler.Type.PAGE
import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.SpyK
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test
import java.net.URL
import java.util.*
import java.util.stream.IntStream
import java.util.stream.Stream

class SequentialStreamsCrawlerTests : AssignmentTests() {
    @SpyK
    var mockCrawler = SequentialStreamsCrawler()

    @MockK
    lateinit var mockElements: CustomArray<WebPageElement>

    @MockK
    lateinit var mockUniqueUris: ConcurrentHashSet<String>

    @MockK
    lateinit var mockImage: Image

    @MockK
    lateinit var mockPageElementStream: Stream<WebPageElement>

    @MockK
    lateinit var mockImageStream: Stream<Image>

    @MockK
    lateinit var mockTransforms: List<Transform>

    @MockK
    lateinit var mockTransformStream: Stream<Transform>

    @MockK
    lateinit var mockTransform: Transform

    private val expected = -99

    @Before
    fun before() {
        MockKAnnotations.init(this, relaxUnitFun = true)
        every { mockCrawler.log(any(), *anyVararg()) } answers { }
        every { mockCrawler.runLocalTransforms() } returns true
        every { mockCrawler.runRemoteTransforms() } returns false
    }

    @Test
    fun `performCrawl() uses expected chained method calls`() {
        mockkStatic(Stream::class)
        val uri = "mock"
        val mockIntStream = mockk<Stream<Int>>()
        val mockUriStream = mockk<Stream<String>>()
        val mockOptional = mockk<Optional<Int>>()

        every { mockOptional.orElse(0) } returns expected
        every { mockUriStream.map<Int>(any()) } returns mockIntStream
        every { mockIntStream.findFirst() } returns mockOptional
        every { Stream.of(uri) } returns mockUriStream
        every { mockUriStream.filter(any()) } returns mockUriStream

        assertThat(mockCrawler.performCrawl(uri, 2)).isEqualTo(expected)

        verify(exactly = 1) {
            mockOptional.orElse(0)
            mockUriStream.map<Int>(any())
            mockUriStream.filter(any())
            Stream.of(uri)
            mockIntStream.findFirst()
        }
    }

    @Test
    fun `performCrawl() stops when maximum depth has been reached`() {
        val uri = "mock"

        mockCrawler.mMaxDepth = 1
        mockCrawler.mUniqueUris = mockUniqueUris

        every {
            mockCrawler.crawlPage(any(), any())
        } throws (RuntimeException("crawlPage should not be called!"))

        every {
            mockCrawler.mUniqueUris.putIfAbsent(any())
        } throws (RuntimeException("mUniqueUris.putIfAbsent() should not be called!"))

        assertThat(mockCrawler.performCrawl(uri, 2)).isEqualTo(0)

        verify(exactly = 0) { mockCrawler.crawlPage(any(), any()) }
        confirmVerified(mockUniqueUris)
    }

    @Test
    fun `performCrawl() runs for all valid depths`() {
        val uri = "mock"
        mockCrawler.mMaxDepth = 3
        mockCrawler.mUniqueUris = mockUniqueUris

        every { mockCrawler.crawlPage(any(), any()) } answers { expected }
        every { mockCrawler.mUniqueUris.putIfAbsent(any()) } answers { true }

        repeat(mockCrawler.mMaxDepth) {
            mockCrawler.performCrawl(uri, it + 1)
        }

        verify(exactly = mockCrawler.mMaxDepth) {
            mockCrawler.performCrawl(any(), any())
            mockUniqueUris.putIfAbsent(any())
            mockCrawler.crawlPage(any(), any())
        }

        confirmVerified(mockUniqueUris)
    }

    @Test
    fun `performCrawl() ignores previously cached uris`() {
        val uri = "mock"

        mockCrawler.mMaxDepth = 3
        mockCrawler.mUniqueUris = mockk<ConcurrentHashSet<String>>()

        every {
            mockCrawler.crawlPage(any(), any())
        } throws (RuntimeException("crawlPage should not be called!"))

        every { mockCrawler.mUniqueUris.putIfAbsent(uri) } returns false

        assertThat(mockCrawler.performCrawl(uri, 3)).isEqualTo(0)

        verify(exactly = 1) { mockCrawler.mUniqueUris.putIfAbsent(uri) }
        confirmVerified(mockCrawler.mUniqueUris)
    }

    @Test
    fun `crawlPage() uses expected chained method calls`() {
        mockkStatic(Stream::class)
        val uri = "mock-url"

        val mockStringStream = mockk<Stream<String>>()
        val mockIntegerStream = mockk<Stream<Int>>()
        val mockPageStream = mockk<Stream<Crawler.Page>>()
        val mockOptional = mockk<Optional<Int>>()
        val mockWebPageCrawler = mockk<WebPageCrawler>()
        mockCrawler.mWebPageCrawler = mockWebPageCrawler

        every { mockStringStream.map<Crawler.Page>(any()) } returns mockPageStream
        every { mockIntegerStream.findFirst() } returns mockOptional
        every { mockPageStream.filter(any()) } returns mockPageStream
        every { mockOptional.orElse(any()) } returns expected;
        every { Stream.of(uri) } returns mockStringStream
        every { mockPageStream.map<Int>(any()) } returns mockIntegerStream

        assertThat(mockCrawler.crawlPage(uri, Int.MAX_VALUE)).isEqualTo(expected)

        verify(exactly = 1) {
            Stream.of(uri)
            mockIntegerStream.findFirst()
            mockStringStream.map<Crawler.Page>(any())
            mockPageStream.map<Int>(any())
            mockOptional.orElse(any())
            mockPageStream.filter(any())
        }
    }

    @Test
    fun `crawlPage() uses expected lambdas`() {
        val uri = "http://www.mock.url/mock-page"
        val mockPage = mockk<Crawler.Page>()
        val mockWebPageCrawler = mockk<WebPageCrawler>()
        mockCrawler.mWebPageCrawler = mockWebPageCrawler

        every { mockWebPageCrawler.getPage(any()) } returns mockPage
        every { mockCrawler.processPage(any(), any()) } returns expected

        assertThat(mockCrawler.crawlPage(uri, Int.MAX_VALUE)).isEqualTo(expected)

        verify(exactly = 1) {
            mockWebPageCrawler.getPage(any())
            mockCrawler.processPage(any(), any())
        }
    }

    @Test
    fun `crawlPage() handles null pages`() {
        val uri = "mock-url"
        val mockWebPageCrawler = mockk<WebPageCrawler>()
        mockCrawler.mWebPageCrawler = mockWebPageCrawler

        every { mockWebPageCrawler.getPage(any()) } returns null
        every { mockCrawler.processPage(any(), any()) } returns 100

        assertThat(mockCrawler.crawlPage(uri, Int.MAX_VALUE)).isEqualTo(0)

        verify(exactly = 0) { mockCrawler.processPage(any(), any()) }
        verify(exactly = 1) { mockWebPageCrawler.getPage(any()) }
    }

    @Test
    fun `processPage() uses expected chained method calls`() {
        val mockPage = mockk<Crawler.Page>()
        val mockIntStream = mockk<IntStream>()

        every { mockIntStream.sum() } returns 98
        every { mockElements.stream() } returns mockPageElementStream
        every { mockPage.getPageElements(any(), any()) } answers {
            val types = it.invocation.args[0] as Array<*>
            assertThat(types[0]).isNotEqualTo(types[1])
            mockElements
        }
        every { mockPageElementStream.mapToInt(any()) } returns mockIntStream

        assertThat(mockCrawler.processPage(mockPage, 99)).isEqualTo(98)

        every { mockPage.getPageElements(any(), any()) } answers {
            val types = it.invocation.args[0] as Array<*>
            assertThat(types[0]).isNotEqualTo(types[1])
            mockElements
        }

        verify(exactly = 1) {
            mockPageElementStream.mapToInt(any())
            mockIntStream.sum()
            mockElements.stream()
        }
    }

    @Test
    fun `processPage() uses expected lambdas`() {
        val spyPage = spyk<Crawler.Page>()
        val mockImageElement = mockk<WebPageElement>()
        val mockPageElement = mockk<WebPageElement>()
        every { mockImageElement.type } returns IMAGE
        every { mockPageElement.type } returns PAGE
        val spyPageElementStream = spyk(Stream.of(mockImageElement, mockPageElement))

        val mockPageElements =
            mockk<edu.vanderbilt.imagecrawler.utils.Array<WebPageElement>>()

        every { spyPage.getPageElements(any(), any()) } returns mockPageElements
        every { mockPageElements.stream() } returns spyPageElementStream
        every { mockCrawler.processImage(any()) } returns expected
        every { mockCrawler.performCrawl(any(), Int.MAX_VALUE) } returns expected

        mockCrawler.processPage(spyPage, Int.MAX_VALUE - 1)

        verify(exactly = 1) {
            spyPage.getPageElements(any(), any())
            mockPageElements.stream()
            mockCrawler.processImage(any())
            spyPageElementStream.mapToInt(any())
            mockCrawler.performCrawl(any(), Int.MAX_VALUE)
        }
    }

    @Test
    fun `processImage() uses expected chained method calls`() {
        mockkStatic(Stream::class)
        val mockURLStream = mockk<Stream<URL>>()
        val mockImageStream = mockk<Stream<Image>>()
        val mockIntStream = mockk<IntStream>()

        val url = mockk<URL>()

        every { mockImageStream.filter(any()) } returns mockImageStream
        every { mockImageStream.mapToInt(any()) } returns mockIntStream
        every { mockIntStream.sum() } returns expected
        every { mockURLStream.map<Image>(any()) } returns mockImageStream
        every { Stream.of(url) } returns mockURLStream

        assertThat(mockCrawler.processImage(url)).isEqualTo(expected)

        verify(exactly = 1) {
            mockURLStream.map<Image>(any())
            mockIntStream.sum()
            Stream.of(url)
            mockImageStream.mapToInt(any())
            mockImageStream.filter(any())
        }
    }

    @Test
    fun `processImage() uses expected lambdas`() {
        val imageUrl = "http://www.mock.com/image"
        val url = URL(imageUrl)

        every { mockCrawler.getOrDownloadImage(url) } returns mockImage
        every { mockCrawler.transformImage(mockImage) } returns expected

        assertThat(mockCrawler.processImage(url)).isEqualTo(expected)

        verify(exactly = 1) {
            mockCrawler.getOrDownloadImage(url)
            mockCrawler.transformImage(mockImage)
        }
    }

    @Test
    fun `processImage() handles downloaded image failure`() {
        val imageUrl = "http://www.mock.com/image"
        val url = URL(imageUrl)

        every { mockCrawler.getOrDownloadImage(url) } returns null
        every { mockCrawler.transformImage(mockImage) } throws
                Exception("transformImage() should not be called")
        assertThat(mockCrawler.processImage(url)).isEqualTo(0)
        verify { mockCrawler.getOrDownloadImage(url) }
    }

    @Test
    fun `processImage() handles no transformed images`() {
        val imageUrl = "http://www.mock.com/image"
        val url = URL(imageUrl)

        every { mockCrawler.getOrDownloadImage(url) } returns mockImage
        every { mockCrawler.transformImage(mockImage) } returns 0

        assertThat(mockCrawler.processImage(url)).isEqualTo(0)

        verify(exactly = 1) {
            mockCrawler.getOrDownloadImage(url)
            mockCrawler.transformImage(mockImage)
        }
    }

    @Test
    fun `transformImage() uses expected chained method calls`() {
        val mockImage = mockk<Image>()

        mockCrawler.mTransforms = mockTransforms

        every { mockTransforms.stream() } returns mockTransformStream
        every { mockTransformStream.map<Image>(any()) } returns mockImageStream
        every { mockImageStream.filter(any()) } returns mockImageStream
        every { mockImageStream.count() } returns expected.toLong()
        every { mockTransformStream.filter(any()) } returns mockTransformStream

        assertThat(mockCrawler.transformImage(mockImage)).isEqualTo(expected)

        verify(exactly = 1) {
            mockImageStream.filter(any())
            mockImageStream.count()
            mockTransforms.stream()
            mockTransformStream.filter(any())
            mockTransformStream.map<Image>(any())
        }
    }

    @Test
    fun `transformImage() uses expected lambdas within chained method calls`() {
        mockCrawler.mTransforms = listOf(mockTransform, mockTransform, mockTransform)
        val count = mockCrawler.mTransforms.size

        every { mockCrawler.createNewCacheItem(any(), mockTransform) } returns true
        every { mockCrawler.applyTransform(mockTransform, mockImage) } returns mockImage

        assertThat(mockCrawler.transformImage(mockImage)).isEqualTo(count)

        verify(exactly = count) {
            mockCrawler.createNewCacheItem(any(), mockTransform)
            mockCrawler.applyTransform(mockTransform, mockImage)
        }
    }

    @Test
    fun `transformImage() ignores previously cached transformed images`() {
        mockCrawler.mTransforms = listOf(mockTransform, mockTransform, mockTransform)
        val count = mockCrawler.mTransforms.size

        every { mockCrawler.createNewCacheItem(any(), mockTransform) } returnsMany listOf(true, false, true)
        every { mockCrawler.applyTransform(mockTransform, mockImage) } returnsMany listOf(mockImage, null)

        assertThat(mockCrawler.transformImage(mockImage)).isEqualTo(count - 2)

        verify(exactly = count) { mockCrawler.createNewCacheItem(any(), mockTransform) }
        verify(exactly = count - 1) { mockCrawler.applyTransform(mockTransform, mockImage) }
    }
}
