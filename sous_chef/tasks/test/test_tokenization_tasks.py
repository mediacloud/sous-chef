import re
import unittest
import spacy_download

from sous_chef.tasks.tokenization_tasks import extract_matching_sentences

FIXTURE_1 = "President Donald Trump’s crypto ventures have added roughly $1 billion to his net worth, a new report claims, all the while the SEC has dropped lawsuits against Coinbase Global Inc. (NASDAQ:COIN), Binance (CRYPTO: BNB), and Kraken. The Trump Crypto Empire Breakdown Trump’s family now controls a massive crypto portfolio built on meme coins, stablecoins, and a pending federally regulated bank application according to a new Politico report. Adding to the portfolio, Trump Media & Technology Group (NASDAQ:DJT), where Trump is the largest shareholder, announced plans to stockpile crypto tokens last year. But the biggest move is World Liberty Financial (CRYPTO: WLFI). The Trump-backed venture applied this month to launch a national trust bank that would directly control billions in customer assets backing its USD1 stablecoin. However, the application puts a Trump-linked business under oversight by the Office of the Comptroller of the Currency—one of Trump’s own regulators. Breaking down the ownership, Trump and his family hold roughly 38% of World Liberty’s holding company. Trump is listed as “co-founder emeritus” while Donald Trump Jr., Eric Trump, and Barron Trump are co-founders. Leading the charge is Zach Witkoff, son of White House envoy Steve Witkoff. The Trump-Driven Regulatory Shift Under the Trump administration, the SEC has dropped high-profile enforcement actions against crypto giants as the Justice Department pulled back on crypto enforcement. Trump pardoned Binance founder Changpeng Zhao months after an Abu Dhabi fund used World Liberty’s stablecoin to invest $2 billion in Binance. On Capitol Hill, Congress is considering sweeping crypto legislation while Trump signed a bill last summer bringing stablecoins into the mainstream financial system. Pushing back, Sen. Elizabeth Warren (D-Mass.) urged the OCC to halt World Liberty’s bank review until Trump eliminates conflicts of interest. The agency declined, calling its process “inherently apolitical.” Why This Matters Now Trump’s crypto holdings create direct exposure to regulatory decisions made by his own appointees. However, Democrats winning the House or Senate in 2026 would shift crypto policy overnight. Image: Shutterstock © 2026 Benzinga.com. Benzinga does not provide investment advice."
FIXTURE_2 = "President Donald Trump is expected to announce Friday his intent to nominate Kevin Warsh to be the next Federal Reserve chair, according to two people involved in the process. Trump, who has been weighing the decision for several months and had narrowed down the list of candidates to four in recent weeks, told reporters on Thursday night he had finalized his pick and would formally make the announcement on Friday. “I’m going to be announcing, I think, a really great choice tomorrow,” Trump said while arriving at the premiere of first lady Melania Trump’s documentary. Trump did not name his selection. Warsh met with Trump at the White House on Thursday, according to a person familiar with the matter, and administration officials have been preparing since for the former Fed governor to be Trump’s nominee. The White House and Warsh didn’t immediately respond to requests for comment. Trump declined to publicly name his choice, and administration officials cautioned that nothing was final until announced by Trump directly. Several officials noted that Trump has shifted his view on the best candidate several times over the course of the process. But Trump appeared set in his decision on Thursday night, and the preparations for a White House announcement were underway, the people said. “It’s going to be somebody that is very respected, somebody that’s known to everybody in the financial world,” Trump said. “And I think it’s going to be a very good choice.” Warsh, National Economic Council Director Kevin Hassett, BlackRock executive Rick Rieder and Fed Governor Christopher Waller were the final four candidates under consideration. Trump held in-person interviews with each of the final four candidates as the final stage of a process that concluded earlier this month. Warsh has long been mentioned by Trump as one of his top candidates for the role and met with Trump in December for his formal interview. The sit-down was viewed positively by Trump’s advisors, and Trump told associates that he thought Warsh did well. He also made clear that Warsh, according to one person who spoke to him about the meeting, “looked the part.” Warsh served as a Fed governor for five years after being nominated by President George W. Bush. He’s been considered for top economic roles in the first and second Trump administrations and was viewed as a possible Treasury secretary pick before Trump tapped Scott Bessent. Warsh was also considered by Trump in 2017 for the Fed chair role, which ultimately went to Jerome Powell. “A lot of people think that this is somebody that could have been there a few years ago,” Trump told reporters Thursday night. Trump quickly soured on Powell in his first term and has attacked and criticized the chair and the Fed relentlessly throughout the first year of his second term."


class TestExtractSentences(unittest.TestCase):

    def setUp(self):
        self._nlp = spacy_download.load_spacy('en_core_web_sm')

    def test_no_inclusion_filters_returns_sentences(self):
        sentences = extract_matching_sentences(self._nlp, FIXTURE_1, inclusion_filters=None)
        # expect multiple sentences and the first should reference the president
        self.assertTrue(len(sentences) == 18)
        self.assertIn("President Donald Trump", sentences[0])

    def test_one_inclusion_filter_matches_one_sentence(self):
        # use a pattern expected to appear in a single sentence in FIXTURE_2
        pattern = re.compile(r"premiere")
        sentences = extract_matching_sentences(self._nlp, FIXTURE_2, inclusion_filters=[pattern])
        self.assertEqual(len(sentences), 1)
        self.assertIn("premiere", sentences[0])

    def test_multiple_inclusion_filters_match_multiple_sentence(self):
        patterns = [re.compile(r"crypto"), re.compile(r"World Liberty"), re.compile(r"stablecoin")]
        sentences = extract_matching_sentences(self._nlp, FIXTURE_1, inclusion_filters=patterns)
        # ensure each pattern matched at least one returned sentence
        self.assertGreaterEqual(len(sentences), 3)
        for pat in patterns:
            self.assertTrue(any(pat.search(s) for s in sentences), f"Pattern {pat.pattern} did not match any sentence")

    def test_one_inclusion_filter_matches_no_sentences(self):
        pattern = re.compile(r"moon*")
        sentences = extract_matching_sentences(self._nlp, FIXTURE_2, inclusion_filters=[pattern])
        self.assertEqual(sentences, [])


if __name__ == "__main__":
    unittest.main()
