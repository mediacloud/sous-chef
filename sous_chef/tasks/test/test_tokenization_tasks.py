import re
import unittest
import spacy_download

from sous_chef.tasks.tokenization_tasks import extract_matching_sentences

FIXTURE_1 = "President Donald Trump’s crypto ventures have added roughly $1 billion to his net worth, a new report claims, all the while the SEC has dropped lawsuits against Coinbase Global Inc. (NASDAQ:COIN), Binance (CRYPTO: BNB), and Kraken. The Trump Crypto Empire Breakdown Trump’s family now controls a massive crypto portfolio built on meme coins, stablecoins, and a pending federally regulated bank application according to a new Politico report. Adding to the portfolio, Trump Media & Technology Group (NASDAQ:DJT), where Trump is the largest shareholder, announced plans to stockpile crypto tokens last year. But the biggest move is World Liberty Financial (CRYPTO: WLFI). The Trump-backed venture applied this month to launch a national trust bank that would directly control billions in customer assets backing its USD1 stablecoin. However, the application puts a Trump-linked business under oversight by the Office of the Comptroller of the Currency—one of Trump’s own regulators. Breaking down the ownership, Trump and his family hold roughly 38% of World Liberty’s holding company. Trump is listed as “co-founder emeritus” while Donald Trump Jr., Eric Trump, and Barron Trump are co-founders. Leading the charge is Zach Witkoff, son of White House envoy Steve Witkoff. The Trump-Driven Regulatory Shift Under the Trump administration, the SEC has dropped high-profile enforcement actions against crypto giants as the Justice Department pulled back on crypto enforcement. Trump pardoned Binance founder Changpeng Zhao months after an Abu Dhabi fund used World Liberty’s stablecoin to invest $2 billion in Binance. On Capitol Hill, Congress is considering sweeping crypto legislation while Trump signed a bill last summer bringing stablecoins into the mainstream financial system. Pushing back, Sen. Elizabeth Warren (D-Mass.) urged the OCC to halt World Liberty’s bank review until Trump eliminates conflicts of interest. The agency declined, calling its process “inherently apolitical.” Why This Matters Now Trump’s crypto holdings create direct exposure to regulatory decisions made by his own appointees. However, Democrats winning the House or Senate in 2026 would shift crypto policy overnight. Image: Shutterstock © 2026 Benzinga.com. Benzinga does not provide investment advice."
FIXTURE_2 = "President Donald Trump is expected to announce Friday his intent to nominate Kevin Warsh to be the next Federal Reserve chair, according to two people involved in the process. Trump, who has been weighing the decision for several months and had narrowed down the list of candidates to four in recent weeks, told reporters on Thursday night he had finalized his pick and would formally make the announcement on Friday. “I’m going to be announcing, I think, a really great choice tomorrow,” Trump said while arriving at the premiere of first lady Melania Trump’s documentary. Trump did not name his selection. Warsh met with Trump at the White House on Thursday, according to a person familiar with the matter, and administration officials have been preparing since for the former Fed governor to be Trump’s nominee. The White House and Warsh didn’t immediately respond to requests for comment. Trump declined to publicly name his choice, and administration officials cautioned that nothing was final until announced by Trump directly. Several officials noted that Trump has shifted his view on the best candidate several times over the course of the process. But Trump appeared set in his decision on Thursday night, and the preparations for a White House announcement were underway, the people said. “It’s going to be somebody that is very respected, somebody that’s known to everybody in the financial world,” Trump said. “And I think it’s going to be a very good choice.” Warsh, National Economic Council Director Kevin Hassett, BlackRock executive Rick Rieder and Fed Governor Christopher Waller were the final four candidates under consideration. Trump held in-person interviews with each of the final four candidates as the final stage of a process that concluded earlier this month. Warsh has long been mentioned by Trump as one of his top candidates for the role and met with Trump in December for his formal interview. The sit-down was viewed positively by Trump’s advisors, and Trump told associates that he thought Warsh did well. He also made clear that Warsh, according to one person who spoke to him about the meeting, “looked the part.” Warsh served as a Fed governor for five years after being nominated by President George W. Bush. He’s been considered for top economic roles in the first and second Trump administrations and was viewed as a possible Treasury secretary pick before Trump tapped Scott Bessent. Warsh was also considered by Trump in 2017 for the Fed chair role, which ultimately went to Jerome Powell. “A lot of people think that this is somebody that could have been there a few years ago,” Trump told reporters Thursday night. Trump quickly soured on Powell in his first term and has attacked and criticized the chair and the Fed relentlessly throughout the first year of his second term."
FIXTURE_3 = """
FACT FOCUS: Images of NYC mayor with Jeffrey Epstein are AI-generated. Here's how we know Multiple AI-generated photos falsely claiming to show New York City Mayor Zohran Mamdani as a child and his mother, filmmaker Mira Nair, with disgraced financier Jeffrey Epstein and his confidant Ghislaine Maxwell, along with other high-profile public figures, were shared widely on social media Monday. Multiple AI-generated photos falsely claiming to show New York City Mayor Zohran Mamdani as a child and his mother, filmmaker Mira Nair, with disgraced financier Jeffrey Epstein and his confidant Ghislaine Maxwell, along with other high-profile public figures, were shared widely on social media Monday. The images originated on an X account labeled as parody after a huge tranche of new Epstein files was released by the Justice Department on Friday. They are clearly watermarked as AI and other elements they contain do not add up. Here's a closer look at the facts. CLAIM: Images show Mamdani as a child and his mother with Jeffrey Epstein and other public figures linked to the disgraced financier. THE FACTS: The images were created with artificial intelligence. They all contain a digital watermark identifying them as such and first appeared on a parody X account that says it creates “high quality AI videos and memes." In one of the images, Mamdani and Nair appear in the front of a group photo with Maxwell, Epstein, former President Bill Clinton, Amazon founder Jeff Bezos and Microsoft founder Bill Gates. They seem to be posing at night on a crowded city street. Mamdani looks to be a preteen or young teenager. Another supposedly shows the same group of people, minus Nair, in what appears to be a tropical setting. Epstein is pictured holding Clinton sitting in his arms, while Maxwell has her arm around Mamdani, who appears slightly younger. Other AI-generated images circulating online depict Mamdani as a baby being held by Nair while she poses with Epstein, Clinton, Maxwell and Bezos. None of Epstein's victims have publicly accused Clinton, Gates or Bezos of being involved in his crimes. Google's Gemini app detected SynthID, a digital watermarking tool for identifying content that has been generated or altered with AI, in all the images described above. This means they were created or edited, either entirely or in part, by Google's AI models. The X account that first posted the images describes itself as “an AI-powered meme engine” that uses “AI to create memes, songs, stories, and visuals that call things exactly how they are — fast, loud, and impossible to ignore.” An inquiry sent to the account went unanswered. However, a post by the account seems to acknowledge that it created the images. “Damn you guys failed,” it reads. "I purposely made him a baby which would technically make this pic 34 years old. Yikes." The photos began circulating after an email emerged in which a publicist, Peggy Siegal, wrote to Epstein about seeing a variety of luminaries, including Clinton, Bezos and Nair, an award-winning Indian filmmaker, at 2009 afterparty for a film held at Maxwell's townhouse. While Mamdani appears as a baby or young child in all of the images, he was 18 in 2009, when Nair is said to have attended the party. The images have led to related falsehoods that have spread online in their wake. For example, one claims that Epstein is Mamdani's father. This is not true — Mamdani's father is Mahmood Mamdani, an anthropology professor at Columbia University. The NYC Mayor’s Office did not respond to a request for comment. ___ Find AP Fact Checks here: https://apnews.com/APFactCheck.
""" # story 5fdf57df00a7f3ccbb07adeead831ed3c23bb7d596ff842aee4dcd8670ac8951

class TestExtractSentences(unittest.TestCase):

    def setUp(self):
        self._nlp = spacy_download.load_spacy('en_core_web_sm')

    def test_no_inclusion_filters_returns_sentences(self):
        sentences = extract_matching_sentences(self._nlp, FIXTURE_1, inclusion_filters=None)
        # expect multiple sentences and the first should reference the president
        self.assertTrue(len(sentences) == 18)
        self.assertIn("President Donald Trump", sentences[0][1])
        for s in sentences:
            self.assertIsInstance(s, tuple)
            self.assertIsInstance(s[0], int)
            self.assertIsInstance(s[1], str)

    def test_one_inclusion_filter_matches_one_sentence(self):
        # use a pattern expected to appear in a single sentence in FIXTURE_2
        pattern = re.compile(r"premiere")
        sentences = extract_matching_sentences(self._nlp, FIXTURE_2, inclusion_filters=[pattern])
        self.assertEqual(len(sentences), 1)
        self.assertIn("premiere", sentences[0][1])

    def test_multiple_inclusion_filters_match_multiple_sentence(self):
        patterns = [re.compile(r"crypto"), re.compile(r"World Liberty"), re.compile(r"stablecoin")]
        sentences = extract_matching_sentences(self._nlp, FIXTURE_1, inclusion_filters=patterns)
        # ensure each pattern matched at least one returned sentence
        self.assertGreaterEqual(len(sentences), 3)
        for pat in patterns:
            self.assertTrue(any(pat.search(s[1]) for s in sentences), f"Pattern {pat.pattern} did not match any sentence")

    def test_one_inclusion_filter_matches_no_sentences(self):
        pattern = re.compile(r"moon*")
        sentences = extract_matching_sentences(self._nlp, FIXTURE_2, inclusion_filters=[pattern])
        self.assertEqual(sentences, [])

    def test_real_input(self):
        pattern = re.compile(r"Mamdani")
        sentences = extract_matching_sentences(self._nlp, FIXTURE_3, inclusion_filters=[pattern])
        self.assertEqual(len(sentences), 10)
        last_index = -1
        for s in sentences:
            self.assertGreaterEqual(s[0], last_index)  # ensure sentences are in order
            self.assertIn("mamdani", s[1].lower())  # verify regex worked
            last_index = s[0]


if __name__ == "__main__":
    unittest.main()
