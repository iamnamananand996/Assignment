-- phpMyAdmin SQL Dump
-- version 4.8.3
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Aug 15, 2019 at 05:48 PM
-- Server version: 10.1.36-MariaDB
-- PHP Version: 5.6.38

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `main_app`
--

-- --------------------------------------------------------

--
-- Table structure for table `campaign`
--

CREATE TABLE `campaign` (
  `campaign_id` int(11) NOT NULL,
  `campaign_name` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `campaign`
--

INSERT INTO `campaign` (`campaign_id`, `campaign_name`) VALUES
(146, 'URLs');

-- --------------------------------------------------------

--
-- Table structure for table `data`
--

CREATE TABLE `data` (
  `json_data` longtext NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `data`
--

INSERT INTO `data` (`json_data`) VALUES
('{\'data\': {\'owner_id\': \'4\', \'campaign\': {\'146\': {\'campaigne_name\': \'URLs\', \'created_date\': \'2019-07-01\', \'url\': {\'6672\': {\'url\': \'https://www.boat.com\', \'domain_data\': {\'page_authority\': 36, \'domain_authority\': 33, \'total_links\': 660739, \'total_internal_links\': 660719, \'total_external_links\': 20, \'NF\': 10, \'DF\': 656892}, \'backlink_data\': {\'follow\': {\'www.walleyecentral.com/\': {\'pa\': 51, \'da\': 43, \'type\': \'Follow\', \'ref_domain\': \'www.walleyecentral.com\', \'anchor_text\': \'boat for sale\\n \\n \\n\\n\\n\\xa0\\n\\n\\n\\n\\n\\n\\n\\n\\xa0\\n\\n\\n\\r\\n\\r\\n\\n\\n\\n\\n\\n\\n.tos_dmca_policy { text-align:center; }\\n.tos_dmca_policy table { display: inline-table; width: auto; }\\n.tos_dmca_policy table { display:inline-table; width: auto; margin: 0 10px; }\', \'expiry_date\': \'\'}, \'www.pensacolafishingforum.com/\': {\'pa\': 50, \'da\': 44, \'type\': \'Follow\', \'ref_domain\': \'www.pensacolafishingforum.com\', \'anchor_text\': \'boat for sale\', \'expiry_date\': \'2020-04-25\'}, \'www.noreast.com/\': {\'pa\': 48, \'da\': 40, \'type\': \'Follow\', \'ref_domain\': \'www.noreast.com\', \'anchor_text\': \'boat for sale\', \'expiry_date\': \'2020-07-28\'}, \'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d189583\': {\'pa\': 45, \'da\': 42, \'type\': \'Follow\', \'ref_domain\': \'www.2coolfishing.com\', \'anchor_text\': \'boat for sale\', \'expiry_date\': \'None\'}, \'www.angelfire.com/crazy3/tj10/\': {\'pa\': 44, \'da\': 91, \'type\': \'Follow\', \'ref_domain\': \'www.angelfire.com\', \'anchor_text\': \'boat.com\', \'expiry_date\': \'2020-10-14\'}, \'walleyecentral.com/\': {\'pa\': 43, \'da\': 43, \'type\': \'Follow\', \'ref_domain\': \'walleyecentral.com\', \'anchor_text\': \'boat for sale\', \'expiry_date\': \'\'}, \'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/celtics_bear_down_to_beat_grizzlies/\': {\'pa\': 42, \'da\': 92, \'type\': \'Follow\', \'ref_domain\': \'archive.boston.com\', \'anchor_text\': \'boat.com\', \'expiry_date\': \'2023-04-07\'}, \'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/an_early_glimpse_of_future/\': {\'pa\': 42, \'da\': 92, \'type\': \'Follow\', \'ref_domain\': \'archive.boston.com\', \'anchor_text\': \'boat.com\', \'expiry_date\': \'2023-04-07\'}, \'forums.noreast.com/\': {\'pa\': 41, \'da\': 40, \'type\': \'Follow\', \'ref_domain\': \'forums.noreast.com\', \'anchor_text\': \'boat for sale\', \'expiry_date\': \'2020-07-28\'}, \'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d220484\': {\'pa\': 41, \'da\': 42, \'type\': \'Follow\', \'ref_domain\': \'www.2coolfishing.com\', \'anchor_text\': \'boat for sale\', \'expiry_date\': \'None\'}}, \'nofollow\': {}}}, \'6671\': {\'url\': \'https://www.datamintelligence.com\', \'domain_data\': {\'page_authority\': 29, \'domain_authority\': 20, \'total_links\': 1836, \'total_internal_links\': 1816, \'total_external_links\': 20, \'NF\': 10, \'DF\': 188}, \'backlink_data\': {\'follow\': {\'www.whatech.com/market-research/companies\': {\'pa\': 45, \'da\': 55, \'type\': \'Follow\', \'ref_domain\': \'www.whatech.com\', \'anchor_text\': \'\', \'expiry_date\': \'2021-01-31\'}, \'www.whatech.com/companies/add/search-by/country/2?value=India\': {\'pa\': 32, \'da\': 55, \'type\': \'Follow\', \'ref_domain\': \'www.whatech.com\', \'anchor_text\': \'\', \'expiry_date\': \'2021-01-31\'}, \'onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-report-published-bydata-m-intelligence-insights-and-forecasts.html/\': {\'pa\': 31, \'da\': 56, \'type\': \'Follow\', \'ref_domain\': \'onlineprnews.com\', \'anchor_text\': \'https://www.datamintelligence.com\', \'expiry_date\': \'2019-10-05\'}, \'www.media.onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-reportpublished-by-data-m-intelligence-insights-and-forecasts.html\': {\'pa\': 31, \'da\': 56, \'type\': \'Follow\', \'ref_domain\': \'www.media.onlineprnews.com\', \'anchor_text\': \'https://www.datamintelligence.com\', \'expiry_date\': \'2019-10-05\'}, \'www.media.onlineprnews.com/news/1128260-1553596061-global-malariavaccines-market-to-witness-higher-rd-activities-for-advanced-drug-development.html\': {\'pa\': 31, \'da\': 56, \'type\': \'Follow\', \'ref_domain\': \'www.media.onlineprnews.com\', \'anchor_text\': \'https://www.datamintelligence.com/\', \'expiry_date\': \'2019-10-05\'}, \'www.media.onlineprnews.com/news/1128333-1553682969-global-clinical-laboratory-services-market-keyfacts-and-insights-to-explore-market-opportunities.html\': {\'pa\': 31, \'da\': 56, \'type\': \'Follow\', \'ref_domain\': \'www.media.onlineprnews.com\', \'anchor_text\': \'http://www.datamintelligence.com/\', \'expiry_date\': \'2019-10-05\'}, \'www.whatech.com/market-research/companies/search-by/tags?value=Research%20reports\': {\'pa\': 29, \'da\': 55, \'type\': \'Follow\', \'ref_domain\': \'www.whatech.com\', \'anchor_text\': \'\', \'expiry_date\': \'2021-01-31\'}, \'www.whatech.com/component/mtree/search-by/tags?value=business\': {\'pa\': 29, \'da\': 55, \'type\': \'Follow\', \'ref_domain\': \'www.whatech.com\', \'anchor_text\': \'\', \'expiry_date\': \'2021-01-31\'}, \'www.whatech.com/it-pages/search-by/tags-keywords?value=DataM%20Intelligence\': {\'pa\': 29, \'da\': 55, \'type\': \'Follow\', \'ref_domain\': \'www.whatech.com\', \'anchor_text\': \'\', \'expiry_date\': \'2021-01-31\'}, \'www.whatech.com/companies/search-by/tags-keywords?value=Research%20reports\': {\'pa\': 29, \'da\': 55, \'type\': \'Follow\', \'ref_domain\': \'www.whatech.com\', \'anchor_text\': \'\', \'expiry_date\': \'2021-01-31\'}}, \'nofollow\': {}}}, \'6651\': {\'url\': \'https://www.stackoverflow.com/\', \'domain_data\': {\'page_authority\': 70, \'domain_authority\': 93, \'total_links\': 415117, \'total_internal_links\': 415097, \'total_external_links\': 20, \'NF\': 10, \'DF\': 405804}, \'backlink_data\': {\'follow\': {\'github.com/alexreisner/geocoder\': {\'pa\': 67, \'da\': 97, \'type\': \'Follow\', \'ref_domain\': \'github.com\', \'anchor_text\': \'stackoverflow\', \'expiry_date\': \'2020-10-09\'}, \'www.reddit.com/r/IAmA/comments/5pouv4/18_months_ago_i_didnt_know_how_to_code_im_now_a/\': {\'pa\': 63, \'da\': 90, \'type\': \'Follow\', \'ref_domain\': \'www.reddit.com\', \'anchor_text\': \'www.stackoverflow.com\', \'expiry_date\': \'2020-04-29\'}, \'www.microsoft.com/enus/download/details.aspx?id=49959\': {\'pa\': 62, \'da\': 100, \'type\': \'Follow\', \'ref_domain\': \'www.microsoft.com\', \'anchor_text\': \'stackoverflow.com\', \'expiry_date\': \'2021-05-02\'}, \'www.reddit.com/r/tifu/comments/5ajym3/tifu_by_punching_my_boss/\': {\'pa\': 61, \'da\': 90, \'type\': \'Follow\', \'ref_domain\': \'www.reddit.com\', \'anchor_text\': \'stack\', \'expiry_date\': \'2020-04-29\'}, \'blogs.msdn.microsoft.com/ericlippert/2009/04/29/events-and-races/\': {\'pa\': 61, \'da\': 100, \'type\': \'Follow\', \'ref_domain\': \'blogs.msdn.microsoft.com\', \'anchor_text\': \'stackoverflow\', \'expiry_date\': \'2021-05-02\'}, \'docs.microsoft.com/en-us/azure/app-service/web-sites-dotnet-troubleshoot-visual-studio\': {\'pa\': 61, \'da\': 100, \'type\': \'Follow\', \'ref_domain\': \'docs.microsoft.com\', \'anchor_text\': \'stackoverflow.com\', \'expiry_date\': \'2021-05-02\'}, \'www.reddit.com/r/AppEngine/comments/h8sue/rip_appengine_xpost_from_rpython/\': {\'pa\': 60, \'da\': 90, \'type\': \'Follow\', \'ref_domain\': \'www.reddit.com\', \'anchor_text\': \'stack overflow\', \'expiry_date\': \'2020-04-29\'}, \'www.reddit.com/r/learnprogramming/comments/6fto5j/how_to_start_learning_to_code_when_you_dont_know/\': {\'pa\': 60, \'da\': 90, \'type\': \'Follow\', \'ref_domain\': \'www.reddit.com\', \'anchor_text\': \'www.stackoverflow.com\', \'expiry_date\': \'2020-04-29\'}, \'www.reddit.com/r/AppEngine/\': {\'pa\': 60, \'da\': 90, \'type\': \'Follow\', \'ref_domain\': \'www.reddit.com\', \'anchor_text\': \'stack overflow\', \'expiry_date\': \'2020-04-29\'}, \'conferences.oreilly.com/oscon/oscon-or\': {\'pa\': 60, \'da\': 92, \'type\': \'Follow\', \'ref_domain\': \'conferences.oreilly.com\', \'anchor_text\': \'stack overflow\', \'expiry_date\': \'2020-05-26\'}}, \'no-follow\': {}}}, \'6637\': {\'url\': \'https://www.kaggle.com/kernels\', \'domain_data\': {\'page_authority\': 52, \'domain_authority\': 72, \'total_links\': 1482, \'total_internal_links\': 1462, \'total_external_links\': 20, \'NF\': 10, \'DF\': 435}, \'backlink_data\': {\'follow\': {\'techcrunch.com/2017/03/07/google-is-acquiring-data-science-communitykaggle/\': {\'pa\': 62, \'da\': 94, \'type\': \'Follow\', \'ref_domain\': \'techcrunch.com\', \'anchor_text\': \'kernels\', \'expiry_date\': \'2021-06-10\'}, \'www.reddit.com/r/dataisbeautiful/comments/80xl66/hey_reddit_im_anthony_goldbloom_founder_of_kaggle/\': {\'pa\': 59, \'da\': 90, \'type\': \'Follow\', \'ref_domain\': \'www.reddit.com\', \'anchor_text\': \'kaggle kernels\', \'expiry_date\': \'2020-04-29\'}, \'www.reddit.com/user/gunxblast\': {\'pa\': 59, \'da\': 90, \'type\': \'Follow\', \'ref_domain\': \'www.reddit.com\', \'anchor_text\': \'kernels\', \'expiry_date\': \'2020-04-29\'}, \'cloud.google.com/blog/products/data-analytics/ethereum-bigquery-public-dataset-smart-contract-analytics\': {\'pa\': 58, \'da\': 90, \'type\': \'Follow\', \'ref_domain\': \'cloud.google.com\', \'anchor_text\': \'kernels\', \'expiry_date\': \'2020-09-13\'}, \'medium.com/feed/@rchang\': {\'pa\': 53, \'da\': 96, \'type\': \'Follow\', \'ref_domain\': \'medium.com\', \'anchor_text\': \'kernels\', \'expiry_date\': \'2020-05-26\'}, \'medium.com/@sAbakumoff/angular-vs-react-text-analysis-of-commit-messages-1cda199f3bdb\': {\'pa\': 52, \'da\': 96, \'type\': \'Follow\', \'ref_domain\': \'medium.com\', \'anchor_text\': \'kaggle kernels\', \'expiry_date\': \'2020-05-26\'}, \'jp.techcrunch.com/2017/03/09/20170307google-is-acquiring-data-science-communitykaggle/\': {\'pa\': 51, \'da\': 94, \'type\': \'Follow\', \'ref_domain\': \'jp.techcrunch.com\', \'anchor_text\': \'????\', \'expiry_date\': \'2021-06-10\'}, \'feeds2.feedburner.com/ObjectPartnersIncBlog\': {\'pa\': 50, \'da\': 95, \'type\': \'Follow\', \'ref_domain\': \'feeds2.feedburner.com\', \'anchor_text\': \'https://www.kaggle.com/kernels\', \'expiry_date\': \'2019-12-01\'}, \'cloud.google.com/blog/bigdata/2018/02/google-cloud-and-ncaa-team-up-for-a-unique-march-madness-competition-hosted-on-kaggle\': {\'pa\': 49, \'da\': 90, \'type\': \'Follow\', \'ref_domain\': \'cloud.google.com\', \'anchor_text\': \'kernels\', \'expiry_date\': \'2020-09-13\'}, \'b.hatena.ne.jp/lanius/%E3%83%87%E3%83%BC%E3%82%BF%E5%88%86%E6%9E%90/\': {\'pa\': 47, \'da\': 93, \'type\': \'Follow\', \'ref_domain\': \'b.hatena.ne.jp\', \'anchor_text\': \'kernels kaggle\', \'expiry_date\': \'\'}}, \'nofollow\': {}}}, \'6551\': {\'url\': \'https://www.twitter.com/\', \'domain_data\': {\'page_authority\': 100, \'domain_authority\': 93, \'total_links\': 483065250, \'total_internal_links\': 483065230, \'total_external_links\': 20, \'NF\': 10, \'DF\': 417000794}, \'backlink_data\': {\'follow\': {\'exuberada.co.vu/\': {\'pa\': 91, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'exuberada.co.vu\', \'anchor_text\': \'twitter\', \'expiry_date\': \'\'}, \'naufragastes.co.vu/\': {\'pa\': 89, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'naufragastes.co.vu\', \'anchor_text\': \'twitter\', \'expiry_date\': \'\'}, \'phakebish.co.vu/\': {\'pa\': 86, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'phakebish.co.vu\', \'anchor_text\': \'twitter\', \'expiry_date\': \'\'}, \'moodbig.tumblr.com/\': {\'pa\': 85, \'da\': 3, \'type\': \'Follow\', \'ref_domain\': \'moodbig.tumblr.com\', \'anchor_text\': \'twitter\', \'expiry_date\': \'2023-06-08\'}, \'merrikat.co.vu/\': {\'pa\': 85, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'merrikat.co.vu\', \'anchor_text\': \'twitter\', \'expiry_date\': \'\'}, \'minhavidasemti.tumblr.com/\': {\'pa\': 84, \'da\': 3, \'type\': \'Follow\', \'ref_domain\': \'minhavidasemti.tumblr.com\', \'anchor_text\': \'twitter\', \'expiry_date\': \'2023-06-08\'}, \'paceywitter.co.vu/\': {\'pa\': 82, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'paceywitter.co.vu\', \'anchor_text\': \'twitter\', \'expiry_date\': \'\'}, \'hyperthot.co.vu/\': {\'pa\': 82, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'hyperthot.co.vu\', \'anchor_text\': \'twitter\', \'expiry_date\': \'\'}, \'childhoodnostalgias.tumblr.com/\': {\'pa\': 82, \'da\': 8, \'type\': \'Follow\', \'ref_domain\': \'childhoodnostalgias.tumblr.com\', \'anchor_text\': \'twitter\', \'expiry_date\': \'2023-06-08\'}, \'moonechika.co.vu/\': {\'pa\': 81, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'moonechika.co.vu\', \'anchor_text\': \'twitter\', \'expiry_date\': \'\'}}, \'no-follow\': {}}}, \'6552\': {\'url\': \'https://www.react.org/\', \'domain_data\': {\'page_authority\': 37, \'domain_authority\': 38, \'total_links\': 5418, \'total_internal_links\': 5398, \'total_external_links\': 20, \'NF\': 10, \'DF\': 5104}, \'backlink_data\': {\'follow\': {\'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/\': {\'pa\': 62, \'da\': 96, \'type\': \'Follow\', \'ref_domain\': \'ec.europa.eu\', \'anchor_text\': \'react\', \'expiry_date\': \'None\'}, \'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/index_en.htm\': {\'pa\': 58, \'da\': 96, \'type\': \'Follow\', \'ref_domain\': \'ec.europa.eu\', \'anchor_text\': \'react\', \'expiry_date\': \'None\'}, \'medium.com/sealnetwork/10-reasons-why-you-should-invest-in-seal-aee71515de27\': {\'pa\': 52, \'da\': 96, \'type\': \'Follow\', \'ref_domain\': \'medium.com\', \'anchor_text\': \'react.org\', \'expiry_date\': \'2020-05-26\'}, \'nos.nl/op3/artikel/2266800-instagram-verwijdert-2000-accounts-die-nepmerkkleding-verkopen.html\': {\'pa\': 44, \'da\': 89, \'type\': \'Follow\', \'ref_domain\': \'nos.nl\', \'anchor_text\': \'react\', \'expiry_date\': \'\'}, \'www.trademark-clearinghouse.com/agents\': {\'pa\': 43, \'da\': 52, \'type\': \'Follow\', \'ref_domain\': \'www.trademark-clearinghouse.com\', \'anchor_text\': \'\', \'expiry_date\': \'2020-05-23\'}, \'www.wipo.int/wipo_magazine/en/2012/06/article_0007.html\': {\'pa\': 43, \'da\': 87, \'type\': \'Follow\', \'ref_domain\': \'www.wipo.int\', \'anchor_text\': \'european anti counterfeiting network\', \'expiry_date\': \'\'}, \'www.dmoz.org/Business/Transportation_and_Logistics/Freight_Forwarding/Europe/\': {\'pa\': 42, \'da\': 87, \'type\': \'Follow\', \'ref_domain\': \'www.dmoz.org\', \'anchor_text\': \'react\', \'expiry_date\': \'2021-01-02\'}, \'tw.autos.search.yahoo.com/search?p=react+to&fr=&fr2=piv-dictionary\': {\'pa\': 41, \'da\': 93, \'type\': \'Follow\', \'ref_domain\': \'tw.autos.search.yahoo.com\', \'anchor_text\': \'react official site\', \'expiry_date\': \'2023-01-18\'}, \'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-shopping\': {\'pa\': 41, \'da\': 93, \'type\': \'Follow\', \'ref_domain\': \'tw.autos.search.yahoo.com\', \'anchor_text\': \'react official site\', \'expiry_date\': \'2023-01-18\'}, \'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-answers\': {\'pa\': 41, \'da\': 93, \'type\': \'Follow\', \'ref_domain\': \'tw.autos.search.yahoo.com\', \'anchor_text\': \'react official site\', \'expiry_date\': \'2023-01-18\'}}, \'no-follow\': {}}}, \'6553\': {\'url\': \'https://www.facebook.com/\', \'domain_data\': {\'page_authority\': 100, \'domain_authority\': 95, \'total_links\': 1295565618, \'total_internal_links\': 1295565598, \'total_external_links\': 20, \'NF\': 10, \'DF\': 1110685279}, \'backlink_data\': {\'follow\': {\'koen.co.vu/\': {\'pa\': 83, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'koen.co.vu\', \'anchor_text\': \'\', \'expiry_date\': \'\'}, \'d.hatena.ne.jp/keyword/Facebook\': {\'pa\': 82, \'da\': 93, \'type\': \'Follow\', \'ref_domain\': \'d.hatena.ne.jp\', \'anchor_text\': \'facebook????????????????\', \'expiry_date\': \'\'}, \'haneun.co.vu/\': {\'pa\': 82, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'haneun.co.vu\', \'anchor_text\': \'facebook\', \'expiry_date\': \'\'}, \'exclamaras.co.vu/\': {\'pa\': 82, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'exclamaras.co.vu\', \'anchor_text\': \'\', \'expiry_date\': \'\'}, \'www.webs.com/\': {\'pa\': 82, \'da\': 88, \'type\': \'Follow\', \'ref_domain\': \'www.webs.com\', \'anchor_text\': \'facebook\', \'expiry_date\': \'\'}, \'www.webs.com/Signup?referer=wftr-fr-3-en\': {\'pa\': 81, \'da\': 88, \'type\': \'Follow\', \'ref_domain\': \'www.webs.com\', \'anchor_text\': \'facebook\', \'expiry_date\': \'\'}, \'millionaire-cartel.tumblr.com/\': {\'pa\': 80, \'da\': 7, \'type\': \'Follow\', \'ref_domain\': \'millionaire-cartel.tumblr.com\', \'anchor_text\': \'i\', \'expiry_date\': \'2023-06-08\'}, \'no-se-quewea-hacer.tumblr.com/\': {\'pa\': 80, \'da\': 9, \'type\': \'Follow\', \'ref_domain\': \'no-sequewea-hacer.tumblr.com\', \'anchor_text\': \'fb\', \'expiry_date\': \'2023-06-08\'}, \'sexifyy.tumblr.com/\': {\'pa\': 80, \'da\': 4, \'type\': \'Follow\', \'ref_domain\': \'sexifyy.tumblr.com\', \'anchor_text\': \'\', \'expiry_date\': \'2023-06-08\'}, \'waitingforastartofall.co.vu/\': {\'pa\': 79, \'da\': 51, \'type\': \'Follow\', \'ref_domain\': \'waitingforastartofall.co.vu\', \'anchor_text\': \'facebook\', \'expiry_date\': \'\'}}, \'no-follow\': {}}}}, \'aggregate_data\': {\'urls\': 7, \'total_follow\': 70, \'total_no_follow\': 0, \'average_pa\': 60.57142857142857, \'average_da\': 63.42857142857143, \'total_backlink\': 70}}}, \'user_domain\': \'https://reactjs.org/\', \'updated_date\': \'2019-07-01\'}}');

-- --------------------------------------------------------

--
-- Table structure for table `owner`
--

CREATE TABLE `owner` (
  `owner_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `owner`
--

INSERT INTO `owner` (`owner_id`) VALUES
(4);

-- --------------------------------------------------------

--
-- Table structure for table `urls`
--

CREATE TABLE `urls` (
  `owner_id` int(11) DEFAULT NULL,
  `campaign_id` int(11) DEFAULT NULL,
  `url_key` int(11) NOT NULL,
  `url_name` varchar(255) DEFAULT NULL,
  `PA` int(11) DEFAULT NULL,
  `DA` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `urls`
--

INSERT INTO `urls` (`owner_id`, `campaign_id`, `url_key`, `url_name`, `PA`, `DA`) VALUES
(4, 146, 6672, 'www.walleyecentral.com/', 51, 43),
(4, 146, 6672, 'www.walleyecentral.com/', 51, 43),
(4, 146, 6672, 'www.walleyecentral.com/', 51, 43),
(4, 146, 6672, 'www.walleyecentral.com/', 51, 43),
(4, 146, 6672, 'www.walleyecentral.com/', 51, 43),
(4, 146, 6672, 'www.walleyecentral.com/', 51, 43),
(4, 146, 6672, 'www.pensacolafishingforum.com/', 50, 44),
(4, 146, 6672, 'www.pensacolafishingforum.com/', 50, 44),
(4, 146, 6672, 'www.pensacolafishingforum.com/', 50, 44),
(4, 146, 6672, 'www.pensacolafishingforum.com/', 50, 44),
(4, 146, 6672, 'www.pensacolafishingforum.com/', 50, 44),
(4, 146, 6672, 'www.pensacolafishingforum.com/', 50, 44),
(4, 146, 6672, 'www.noreast.com/', 48, 40),
(4, 146, 6672, 'www.noreast.com/', 48, 40),
(4, 146, 6672, 'www.noreast.com/', 48, 40),
(4, 146, 6672, 'www.noreast.com/', 48, 40),
(4, 146, 6672, 'www.noreast.com/', 48, 40),
(4, 146, 6672, 'www.noreast.com/', 48, 40),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d189583', 45, 42),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d189583', 45, 42),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d189583', 45, 42),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d189583', 45, 42),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d189583', 45, 42),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d189583', 45, 42),
(4, 146, 6672, 'www.angelfire.com/crazy3/tj10/', 44, 91),
(4, 146, 6672, 'www.angelfire.com/crazy3/tj10/', 44, 91),
(4, 146, 6672, 'www.angelfire.com/crazy3/tj10/', 44, 91),
(4, 146, 6672, 'www.angelfire.com/crazy3/tj10/', 44, 91),
(4, 146, 6672, 'www.angelfire.com/crazy3/tj10/', 44, 91),
(4, 146, 6672, 'www.angelfire.com/crazy3/tj10/', 44, 91),
(4, 146, 6672, 'walleyecentral.com/', 43, 43),
(4, 146, 6672, 'walleyecentral.com/', 43, 43),
(4, 146, 6672, 'walleyecentral.com/', 43, 43),
(4, 146, 6672, 'walleyecentral.com/', 43, 43),
(4, 146, 6672, 'walleyecentral.com/', 43, 43),
(4, 146, 6672, 'walleyecentral.com/', 43, 43),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/celtics_bear_down_to_beat_grizzlies/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/celtics_bear_down_to_beat_grizzlies/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/celtics_bear_down_to_beat_grizzlies/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/celtics_bear_down_to_beat_grizzlies/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/celtics_bear_down_to_beat_grizzlies/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/celtics_bear_down_to_beat_grizzlies/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/an_early_glimpse_of_future/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/an_early_glimpse_of_future/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/an_early_glimpse_of_future/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/an_early_glimpse_of_future/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/an_early_glimpse_of_future/', 42, 92),
(4, 146, 6672, 'archive.boston.com/sports/basketball/celtics/articles/2005/02/17/an_early_glimpse_of_future/', 42, 92),
(4, 146, 6672, 'forums.noreast.com/', 41, 40),
(4, 146, 6672, 'forums.noreast.com/', 41, 40),
(4, 146, 6672, 'forums.noreast.com/', 41, 40),
(4, 146, 6672, 'forums.noreast.com/', 41, 40),
(4, 146, 6672, 'forums.noreast.com/', 41, 40),
(4, 146, 6672, 'forums.noreast.com/', 41, 40),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d220484', 41, 42),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d220484', 41, 42),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d220484', 41, 42),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d220484', 41, 42),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d220484', 41, 42),
(4, 146, 6672, 'www.2coolfishing.com/ttmbforum/showthread.php?t%5Cu003d220484', 41, 42),
(4, 146, 6671, 'www.whatech.com/market-research/companies', 45, 55),
(4, 146, 6671, 'www.whatech.com/market-research/companies', 45, 55),
(4, 146, 6671, 'www.whatech.com/market-research/companies', 45, 55),
(4, 146, 6671, 'www.whatech.com/market-research/companies', 45, 55),
(4, 146, 6671, 'www.whatech.com/market-research/companies', 45, 55),
(4, 146, 6671, 'www.whatech.com/market-research/companies', 45, 55),
(4, 146, 6671, 'www.whatech.com/companies/add/search-by/country/2?value=India', 32, 55),
(4, 146, 6671, 'www.whatech.com/companies/add/search-by/country/2?value=India', 32, 55),
(4, 146, 6671, 'www.whatech.com/companies/add/search-by/country/2?value=India', 32, 55),
(4, 146, 6671, 'www.whatech.com/companies/add/search-by/country/2?value=India', 32, 55),
(4, 146, 6671, 'www.whatech.com/companies/add/search-by/country/2?value=India', 32, 55),
(4, 146, 6671, 'www.whatech.com/companies/add/search-by/country/2?value=India', 32, 55),
(4, 146, 6671, 'onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-report-published-bydata-m-intelligence-insights-and-forecasts.html/', 31, 56),
(4, 146, 6671, 'onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-report-published-bydata-m-intelligence-insights-and-forecasts.html/', 31, 56),
(4, 146, 6671, 'onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-report-published-bydata-m-intelligence-insights-and-forecasts.html/', 31, 56),
(4, 146, 6671, 'onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-report-published-bydata-m-intelligence-insights-and-forecasts.html/', 31, 56),
(4, 146, 6671, 'onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-report-published-bydata-m-intelligence-insights-and-forecasts.html/', 31, 56),
(4, 146, 6671, 'onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-report-published-bydata-m-intelligence-insights-and-forecasts.html/', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-reportpublished-by-data-m-intelligence-insights-and-forecasts.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-reportpublished-by-data-m-intelligence-insights-and-forecasts.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-reportpublished-by-data-m-intelligence-insights-and-forecasts.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-reportpublished-by-data-m-intelligence-insights-and-forecasts.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-reportpublished-by-data-m-intelligence-insights-and-forecasts.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1076660-1520330689-global-glaucoma-therapeutics-market-reportpublished-by-data-m-intelligence-insights-and-forecasts.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128260-1553596061-global-malariavaccines-market-to-witness-higher-rd-activities-for-advanced-drug-development.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128260-1553596061-global-malariavaccines-market-to-witness-higher-rd-activities-for-advanced-drug-development.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128260-1553596061-global-malariavaccines-market-to-witness-higher-rd-activities-for-advanced-drug-development.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128260-1553596061-global-malariavaccines-market-to-witness-higher-rd-activities-for-advanced-drug-development.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128260-1553596061-global-malariavaccines-market-to-witness-higher-rd-activities-for-advanced-drug-development.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128260-1553596061-global-malariavaccines-market-to-witness-higher-rd-activities-for-advanced-drug-development.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128333-1553682969-global-clinical-laboratory-services-market-keyfacts-and-insights-to-explore-market-opportunities.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128333-1553682969-global-clinical-laboratory-services-market-keyfacts-and-insights-to-explore-market-opportunities.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128333-1553682969-global-clinical-laboratory-services-market-keyfacts-and-insights-to-explore-market-opportunities.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128333-1553682969-global-clinical-laboratory-services-market-keyfacts-and-insights-to-explore-market-opportunities.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128333-1553682969-global-clinical-laboratory-services-market-keyfacts-and-insights-to-explore-market-opportunities.html', 31, 56),
(4, 146, 6671, 'www.media.onlineprnews.com/news/1128333-1553682969-global-clinical-laboratory-services-market-keyfacts-and-insights-to-explore-market-opportunities.html', 31, 56),
(4, 146, 6671, 'www.whatech.com/market-research/companies/search-by/tags?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/market-research/companies/search-by/tags?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/market-research/companies/search-by/tags?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/market-research/companies/search-by/tags?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/market-research/companies/search-by/tags?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/market-research/companies/search-by/tags?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/component/mtree/search-by/tags?value=business', 29, 55),
(4, 146, 6671, 'www.whatech.com/component/mtree/search-by/tags?value=business', 29, 55),
(4, 146, 6671, 'www.whatech.com/component/mtree/search-by/tags?value=business', 29, 55),
(4, 146, 6671, 'www.whatech.com/component/mtree/search-by/tags?value=business', 29, 55),
(4, 146, 6671, 'www.whatech.com/component/mtree/search-by/tags?value=business', 29, 55),
(4, 146, 6671, 'www.whatech.com/component/mtree/search-by/tags?value=business', 29, 55),
(4, 146, 6671, 'www.whatech.com/it-pages/search-by/tags-keywords?value=DataM%20Intelligence', 29, 55),
(4, 146, 6671, 'www.whatech.com/it-pages/search-by/tags-keywords?value=DataM%20Intelligence', 29, 55),
(4, 146, 6671, 'www.whatech.com/it-pages/search-by/tags-keywords?value=DataM%20Intelligence', 29, 55),
(4, 146, 6671, 'www.whatech.com/it-pages/search-by/tags-keywords?value=DataM%20Intelligence', 29, 55),
(4, 146, 6671, 'www.whatech.com/it-pages/search-by/tags-keywords?value=DataM%20Intelligence', 29, 55),
(4, 146, 6671, 'www.whatech.com/it-pages/search-by/tags-keywords?value=DataM%20Intelligence', 29, 55),
(4, 146, 6671, 'www.whatech.com/companies/search-by/tags-keywords?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/companies/search-by/tags-keywords?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/companies/search-by/tags-keywords?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/companies/search-by/tags-keywords?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/companies/search-by/tags-keywords?value=Research%20reports', 29, 55),
(4, 146, 6671, 'www.whatech.com/companies/search-by/tags-keywords?value=Research%20reports', 29, 55),
(4, 146, 6651, 'github.com/alexreisner/geocoder', 67, 97),
(4, 146, 6651, 'github.com/alexreisner/geocoder', 67, 97),
(4, 146, 6651, 'github.com/alexreisner/geocoder', 67, 97),
(4, 146, 6651, 'github.com/alexreisner/geocoder', 67, 97),
(4, 146, 6651, 'github.com/alexreisner/geocoder', 67, 97),
(4, 146, 6651, 'github.com/alexreisner/geocoder', 67, 97),
(4, 146, 6651, 'www.reddit.com/r/IAmA/comments/5pouv4/18_months_ago_i_didnt_know_how_to_code_im_now_a/', 63, 90),
(4, 146, 6651, 'www.reddit.com/r/IAmA/comments/5pouv4/18_months_ago_i_didnt_know_how_to_code_im_now_a/', 63, 90),
(4, 146, 6651, 'www.reddit.com/r/IAmA/comments/5pouv4/18_months_ago_i_didnt_know_how_to_code_im_now_a/', 63, 90),
(4, 146, 6651, 'www.reddit.com/r/IAmA/comments/5pouv4/18_months_ago_i_didnt_know_how_to_code_im_now_a/', 63, 90),
(4, 146, 6651, 'www.reddit.com/r/IAmA/comments/5pouv4/18_months_ago_i_didnt_know_how_to_code_im_now_a/', 63, 90),
(4, 146, 6651, 'www.reddit.com/r/IAmA/comments/5pouv4/18_months_ago_i_didnt_know_how_to_code_im_now_a/', 63, 90),
(4, 146, 6651, 'www.microsoft.com/enus/download/details.aspx?id=49959', 62, 100),
(4, 146, 6651, 'www.microsoft.com/enus/download/details.aspx?id=49959', 62, 100),
(4, 146, 6651, 'www.microsoft.com/enus/download/details.aspx?id=49959', 62, 100),
(4, 146, 6651, 'www.microsoft.com/enus/download/details.aspx?id=49959', 62, 100),
(4, 146, 6651, 'www.microsoft.com/enus/download/details.aspx?id=49959', 62, 100),
(4, 146, 6651, 'www.microsoft.com/enus/download/details.aspx?id=49959', 62, 100),
(4, 146, 6651, 'www.reddit.com/r/tifu/comments/5ajym3/tifu_by_punching_my_boss/', 61, 90),
(4, 146, 6651, 'www.reddit.com/r/tifu/comments/5ajym3/tifu_by_punching_my_boss/', 61, 90),
(4, 146, 6651, 'www.reddit.com/r/tifu/comments/5ajym3/tifu_by_punching_my_boss/', 61, 90),
(4, 146, 6651, 'www.reddit.com/r/tifu/comments/5ajym3/tifu_by_punching_my_boss/', 61, 90),
(4, 146, 6651, 'www.reddit.com/r/tifu/comments/5ajym3/tifu_by_punching_my_boss/', 61, 90),
(4, 146, 6651, 'www.reddit.com/r/tifu/comments/5ajym3/tifu_by_punching_my_boss/', 61, 90),
(4, 146, 6651, 'blogs.msdn.microsoft.com/ericlippert/2009/04/29/events-and-races/', 61, 100),
(4, 146, 6651, 'blogs.msdn.microsoft.com/ericlippert/2009/04/29/events-and-races/', 61, 100),
(4, 146, 6651, 'blogs.msdn.microsoft.com/ericlippert/2009/04/29/events-and-races/', 61, 100),
(4, 146, 6651, 'blogs.msdn.microsoft.com/ericlippert/2009/04/29/events-and-races/', 61, 100),
(4, 146, 6651, 'blogs.msdn.microsoft.com/ericlippert/2009/04/29/events-and-races/', 61, 100),
(4, 146, 6651, 'blogs.msdn.microsoft.com/ericlippert/2009/04/29/events-and-races/', 61, 100),
(4, 146, 6651, 'docs.microsoft.com/en-us/azure/app-service/web-sites-dotnet-troubleshoot-visual-studio', 61, 100),
(4, 146, 6651, 'docs.microsoft.com/en-us/azure/app-service/web-sites-dotnet-troubleshoot-visual-studio', 61, 100),
(4, 146, 6651, 'docs.microsoft.com/en-us/azure/app-service/web-sites-dotnet-troubleshoot-visual-studio', 61, 100),
(4, 146, 6651, 'docs.microsoft.com/en-us/azure/app-service/web-sites-dotnet-troubleshoot-visual-studio', 61, 100),
(4, 146, 6651, 'docs.microsoft.com/en-us/azure/app-service/web-sites-dotnet-troubleshoot-visual-studio', 61, 100),
(4, 146, 6651, 'docs.microsoft.com/en-us/azure/app-service/web-sites-dotnet-troubleshoot-visual-studio', 61, 100),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/comments/h8sue/rip_appengine_xpost_from_rpython/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/comments/h8sue/rip_appengine_xpost_from_rpython/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/comments/h8sue/rip_appengine_xpost_from_rpython/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/comments/h8sue/rip_appengine_xpost_from_rpython/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/comments/h8sue/rip_appengine_xpost_from_rpython/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/comments/h8sue/rip_appengine_xpost_from_rpython/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/learnprogramming/comments/6fto5j/how_to_start_learning_to_code_when_you_dont_know/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/learnprogramming/comments/6fto5j/how_to_start_learning_to_code_when_you_dont_know/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/learnprogramming/comments/6fto5j/how_to_start_learning_to_code_when_you_dont_know/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/learnprogramming/comments/6fto5j/how_to_start_learning_to_code_when_you_dont_know/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/learnprogramming/comments/6fto5j/how_to_start_learning_to_code_when_you_dont_know/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/learnprogramming/comments/6fto5j/how_to_start_learning_to_code_when_you_dont_know/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/', 60, 90),
(4, 146, 6651, 'www.reddit.com/r/AppEngine/', 60, 90),
(4, 146, 6651, 'conferences.oreilly.com/oscon/oscon-or', 60, 92),
(4, 146, 6651, 'conferences.oreilly.com/oscon/oscon-or', 60, 92),
(4, 146, 6651, 'conferences.oreilly.com/oscon/oscon-or', 60, 92),
(4, 146, 6651, 'conferences.oreilly.com/oscon/oscon-or', 60, 92),
(4, 146, 6651, 'conferences.oreilly.com/oscon/oscon-or', 60, 92),
(4, 146, 6651, 'conferences.oreilly.com/oscon/oscon-or', 60, 92),
(4, 146, 6637, 'techcrunch.com/2017/03/07/google-is-acquiring-data-science-communitykaggle/', 62, 94),
(4, 146, 6637, 'techcrunch.com/2017/03/07/google-is-acquiring-data-science-communitykaggle/', 62, 94),
(4, 146, 6637, 'techcrunch.com/2017/03/07/google-is-acquiring-data-science-communitykaggle/', 62, 94),
(4, 146, 6637, 'techcrunch.com/2017/03/07/google-is-acquiring-data-science-communitykaggle/', 62, 94),
(4, 146, 6637, 'techcrunch.com/2017/03/07/google-is-acquiring-data-science-communitykaggle/', 62, 94),
(4, 146, 6637, 'techcrunch.com/2017/03/07/google-is-acquiring-data-science-communitykaggle/', 62, 94),
(4, 146, 6637, 'www.reddit.com/r/dataisbeautiful/comments/80xl66/hey_reddit_im_anthony_goldbloom_founder_of_kaggle/', 59, 90),
(4, 146, 6637, 'www.reddit.com/r/dataisbeautiful/comments/80xl66/hey_reddit_im_anthony_goldbloom_founder_of_kaggle/', 59, 90),
(4, 146, 6637, 'www.reddit.com/r/dataisbeautiful/comments/80xl66/hey_reddit_im_anthony_goldbloom_founder_of_kaggle/', 59, 90),
(4, 146, 6637, 'www.reddit.com/r/dataisbeautiful/comments/80xl66/hey_reddit_im_anthony_goldbloom_founder_of_kaggle/', 59, 90),
(4, 146, 6637, 'www.reddit.com/r/dataisbeautiful/comments/80xl66/hey_reddit_im_anthony_goldbloom_founder_of_kaggle/', 59, 90),
(4, 146, 6637, 'www.reddit.com/r/dataisbeautiful/comments/80xl66/hey_reddit_im_anthony_goldbloom_founder_of_kaggle/', 59, 90),
(4, 146, 6637, 'www.reddit.com/user/gunxblast', 59, 90),
(4, 146, 6637, 'www.reddit.com/user/gunxblast', 59, 90),
(4, 146, 6637, 'www.reddit.com/user/gunxblast', 59, 90),
(4, 146, 6637, 'www.reddit.com/user/gunxblast', 59, 90),
(4, 146, 6637, 'www.reddit.com/user/gunxblast', 59, 90),
(4, 146, 6637, 'www.reddit.com/user/gunxblast', 59, 90),
(4, 146, 6637, 'cloud.google.com/blog/products/data-analytics/ethereum-bigquery-public-dataset-smart-contract-analytics', 58, 90),
(4, 146, 6637, 'cloud.google.com/blog/products/data-analytics/ethereum-bigquery-public-dataset-smart-contract-analytics', 58, 90),
(4, 146, 6637, 'cloud.google.com/blog/products/data-analytics/ethereum-bigquery-public-dataset-smart-contract-analytics', 58, 90),
(4, 146, 6637, 'cloud.google.com/blog/products/data-analytics/ethereum-bigquery-public-dataset-smart-contract-analytics', 58, 90),
(4, 146, 6637, 'cloud.google.com/blog/products/data-analytics/ethereum-bigquery-public-dataset-smart-contract-analytics', 58, 90),
(4, 146, 6637, 'cloud.google.com/blog/products/data-analytics/ethereum-bigquery-public-dataset-smart-contract-analytics', 58, 90),
(4, 146, 6637, 'medium.com/feed/@rchang', 53, 96),
(4, 146, 6637, 'medium.com/feed/@rchang', 53, 96),
(4, 146, 6637, 'medium.com/feed/@rchang', 53, 96),
(4, 146, 6637, 'medium.com/feed/@rchang', 53, 96),
(4, 146, 6637, 'medium.com/feed/@rchang', 53, 96),
(4, 146, 6637, 'medium.com/feed/@rchang', 53, 96),
(4, 146, 6637, 'medium.com/@sAbakumoff/angular-vs-react-text-analysis-of-commit-messages-1cda199f3bdb', 52, 96),
(4, 146, 6637, 'medium.com/@sAbakumoff/angular-vs-react-text-analysis-of-commit-messages-1cda199f3bdb', 52, 96),
(4, 146, 6637, 'medium.com/@sAbakumoff/angular-vs-react-text-analysis-of-commit-messages-1cda199f3bdb', 52, 96),
(4, 146, 6637, 'medium.com/@sAbakumoff/angular-vs-react-text-analysis-of-commit-messages-1cda199f3bdb', 52, 96),
(4, 146, 6637, 'medium.com/@sAbakumoff/angular-vs-react-text-analysis-of-commit-messages-1cda199f3bdb', 52, 96),
(4, 146, 6637, 'medium.com/@sAbakumoff/angular-vs-react-text-analysis-of-commit-messages-1cda199f3bdb', 52, 96),
(4, 146, 6637, 'jp.techcrunch.com/2017/03/09/20170307google-is-acquiring-data-science-communitykaggle/', 51, 94),
(4, 146, 6637, 'jp.techcrunch.com/2017/03/09/20170307google-is-acquiring-data-science-communitykaggle/', 51, 94),
(4, 146, 6637, 'jp.techcrunch.com/2017/03/09/20170307google-is-acquiring-data-science-communitykaggle/', 51, 94),
(4, 146, 6637, 'jp.techcrunch.com/2017/03/09/20170307google-is-acquiring-data-science-communitykaggle/', 51, 94),
(4, 146, 6637, 'jp.techcrunch.com/2017/03/09/20170307google-is-acquiring-data-science-communitykaggle/', 51, 94),
(4, 146, 6637, 'jp.techcrunch.com/2017/03/09/20170307google-is-acquiring-data-science-communitykaggle/', 51, 94),
(4, 146, 6637, 'feeds2.feedburner.com/ObjectPartnersIncBlog', 50, 95),
(4, 146, 6637, 'feeds2.feedburner.com/ObjectPartnersIncBlog', 50, 95),
(4, 146, 6637, 'feeds2.feedburner.com/ObjectPartnersIncBlog', 50, 95),
(4, 146, 6637, 'feeds2.feedburner.com/ObjectPartnersIncBlog', 50, 95),
(4, 146, 6637, 'feeds2.feedburner.com/ObjectPartnersIncBlog', 50, 95),
(4, 146, 6637, 'feeds2.feedburner.com/ObjectPartnersIncBlog', 50, 95),
(4, 146, 6637, 'cloud.google.com/blog/bigdata/2018/02/google-cloud-and-ncaa-team-up-for-a-unique-march-madness-competition-hosted-on-kaggle', 49, 90),
(4, 146, 6637, 'cloud.google.com/blog/bigdata/2018/02/google-cloud-and-ncaa-team-up-for-a-unique-march-madness-competition-hosted-on-kaggle', 49, 90),
(4, 146, 6637, 'cloud.google.com/blog/bigdata/2018/02/google-cloud-and-ncaa-team-up-for-a-unique-march-madness-competition-hosted-on-kaggle', 49, 90),
(4, 146, 6637, 'cloud.google.com/blog/bigdata/2018/02/google-cloud-and-ncaa-team-up-for-a-unique-march-madness-competition-hosted-on-kaggle', 49, 90),
(4, 146, 6637, 'cloud.google.com/blog/bigdata/2018/02/google-cloud-and-ncaa-team-up-for-a-unique-march-madness-competition-hosted-on-kaggle', 49, 90),
(4, 146, 6637, 'cloud.google.com/blog/bigdata/2018/02/google-cloud-and-ncaa-team-up-for-a-unique-march-madness-competition-hosted-on-kaggle', 49, 90),
(4, 146, 6637, 'b.hatena.ne.jp/lanius/%E3%83%87%E3%83%BC%E3%82%BF%E5%88%86%E6%9E%90/', 47, 93),
(4, 146, 6637, 'b.hatena.ne.jp/lanius/%E3%83%87%E3%83%BC%E3%82%BF%E5%88%86%E6%9E%90/', 47, 93),
(4, 146, 6637, 'b.hatena.ne.jp/lanius/%E3%83%87%E3%83%BC%E3%82%BF%E5%88%86%E6%9E%90/', 47, 93),
(4, 146, 6637, 'b.hatena.ne.jp/lanius/%E3%83%87%E3%83%BC%E3%82%BF%E5%88%86%E6%9E%90/', 47, 93),
(4, 146, 6637, 'b.hatena.ne.jp/lanius/%E3%83%87%E3%83%BC%E3%82%BF%E5%88%86%E6%9E%90/', 47, 93),
(4, 146, 6637, 'b.hatena.ne.jp/lanius/%E3%83%87%E3%83%BC%E3%82%BF%E5%88%86%E6%9E%90/', 47, 93),
(4, 146, 6551, 'exuberada.co.vu/', 91, 51),
(4, 146, 6551, 'exuberada.co.vu/', 91, 51),
(4, 146, 6551, 'exuberada.co.vu/', 91, 51),
(4, 146, 6551, 'exuberada.co.vu/', 91, 51),
(4, 146, 6551, 'exuberada.co.vu/', 91, 51),
(4, 146, 6551, 'exuberada.co.vu/', 91, 51),
(4, 146, 6551, 'naufragastes.co.vu/', 89, 51),
(4, 146, 6551, 'naufragastes.co.vu/', 89, 51),
(4, 146, 6551, 'naufragastes.co.vu/', 89, 51),
(4, 146, 6551, 'naufragastes.co.vu/', 89, 51),
(4, 146, 6551, 'naufragastes.co.vu/', 89, 51),
(4, 146, 6551, 'naufragastes.co.vu/', 89, 51),
(4, 146, 6551, 'phakebish.co.vu/', 86, 51),
(4, 146, 6551, 'phakebish.co.vu/', 86, 51),
(4, 146, 6551, 'phakebish.co.vu/', 86, 51),
(4, 146, 6551, 'phakebish.co.vu/', 86, 51),
(4, 146, 6551, 'phakebish.co.vu/', 86, 51),
(4, 146, 6551, 'phakebish.co.vu/', 86, 51),
(4, 146, 6551, 'moodbig.tumblr.com/', 85, 3),
(4, 146, 6551, 'moodbig.tumblr.com/', 85, 3),
(4, 146, 6551, 'moodbig.tumblr.com/', 85, 3),
(4, 146, 6551, 'moodbig.tumblr.com/', 85, 3),
(4, 146, 6551, 'moodbig.tumblr.com/', 85, 3),
(4, 146, 6551, 'moodbig.tumblr.com/', 85, 3),
(4, 146, 6551, 'merrikat.co.vu/', 85, 51),
(4, 146, 6551, 'merrikat.co.vu/', 85, 51),
(4, 146, 6551, 'merrikat.co.vu/', 85, 51),
(4, 146, 6551, 'merrikat.co.vu/', 85, 51),
(4, 146, 6551, 'merrikat.co.vu/', 85, 51),
(4, 146, 6551, 'merrikat.co.vu/', 85, 51),
(4, 146, 6551, 'minhavidasemti.tumblr.com/', 84, 3),
(4, 146, 6551, 'minhavidasemti.tumblr.com/', 84, 3),
(4, 146, 6551, 'minhavidasemti.tumblr.com/', 84, 3),
(4, 146, 6551, 'minhavidasemti.tumblr.com/', 84, 3),
(4, 146, 6551, 'minhavidasemti.tumblr.com/', 84, 3),
(4, 146, 6551, 'minhavidasemti.tumblr.com/', 84, 3),
(4, 146, 6551, 'paceywitter.co.vu/', 82, 51),
(4, 146, 6551, 'paceywitter.co.vu/', 82, 51),
(4, 146, 6551, 'paceywitter.co.vu/', 82, 51),
(4, 146, 6551, 'paceywitter.co.vu/', 82, 51),
(4, 146, 6551, 'paceywitter.co.vu/', 82, 51),
(4, 146, 6551, 'paceywitter.co.vu/', 82, 51),
(4, 146, 6551, 'hyperthot.co.vu/', 82, 51),
(4, 146, 6551, 'hyperthot.co.vu/', 82, 51),
(4, 146, 6551, 'hyperthot.co.vu/', 82, 51),
(4, 146, 6551, 'hyperthot.co.vu/', 82, 51),
(4, 146, 6551, 'hyperthot.co.vu/', 82, 51),
(4, 146, 6551, 'hyperthot.co.vu/', 82, 51),
(4, 146, 6551, 'childhoodnostalgias.tumblr.com/', 82, 8),
(4, 146, 6551, 'childhoodnostalgias.tumblr.com/', 82, 8),
(4, 146, 6551, 'childhoodnostalgias.tumblr.com/', 82, 8),
(4, 146, 6551, 'childhoodnostalgias.tumblr.com/', 82, 8),
(4, 146, 6551, 'childhoodnostalgias.tumblr.com/', 82, 8),
(4, 146, 6551, 'childhoodnostalgias.tumblr.com/', 82, 8),
(4, 146, 6551, 'moonechika.co.vu/', 81, 51),
(4, 146, 6551, 'moonechika.co.vu/', 81, 51),
(4, 146, 6551, 'moonechika.co.vu/', 81, 51),
(4, 146, 6551, 'moonechika.co.vu/', 81, 51),
(4, 146, 6551, 'moonechika.co.vu/', 81, 51),
(4, 146, 6551, 'moonechika.co.vu/', 81, 51),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/', 62, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/', 62, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/', 62, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/', 62, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/', 62, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/', 62, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/index_en.htm', 58, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/index_en.htm', 58, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/index_en.htm', 58, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/index_en.htm', 58, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/index_en.htm', 58, 96),
(4, 146, 6552, 'ec.europa.eu/trade/policy/accessing-markets/intellectual-property/index_en.htm', 58, 96),
(4, 146, 6552, 'medium.com/sealnetwork/10-reasons-why-you-should-invest-in-seal-aee71515de27', 52, 96),
(4, 146, 6552, 'medium.com/sealnetwork/10-reasons-why-you-should-invest-in-seal-aee71515de27', 52, 96),
(4, 146, 6552, 'medium.com/sealnetwork/10-reasons-why-you-should-invest-in-seal-aee71515de27', 52, 96),
(4, 146, 6552, 'medium.com/sealnetwork/10-reasons-why-you-should-invest-in-seal-aee71515de27', 52, 96),
(4, 146, 6552, 'medium.com/sealnetwork/10-reasons-why-you-should-invest-in-seal-aee71515de27', 52, 96),
(4, 146, 6552, 'medium.com/sealnetwork/10-reasons-why-you-should-invest-in-seal-aee71515de27', 52, 96),
(4, 146, 6552, 'nos.nl/op3/artikel/2266800-instagram-verwijdert-2000-accounts-die-nepmerkkleding-verkopen.html', 44, 89),
(4, 146, 6552, 'nos.nl/op3/artikel/2266800-instagram-verwijdert-2000-accounts-die-nepmerkkleding-verkopen.html', 44, 89),
(4, 146, 6552, 'nos.nl/op3/artikel/2266800-instagram-verwijdert-2000-accounts-die-nepmerkkleding-verkopen.html', 44, 89),
(4, 146, 6552, 'nos.nl/op3/artikel/2266800-instagram-verwijdert-2000-accounts-die-nepmerkkleding-verkopen.html', 44, 89),
(4, 146, 6552, 'nos.nl/op3/artikel/2266800-instagram-verwijdert-2000-accounts-die-nepmerkkleding-verkopen.html', 44, 89),
(4, 146, 6552, 'nos.nl/op3/artikel/2266800-instagram-verwijdert-2000-accounts-die-nepmerkkleding-verkopen.html', 44, 89),
(4, 146, 6552, 'www.trademark-clearinghouse.com/agents', 43, 52),
(4, 146, 6552, 'www.trademark-clearinghouse.com/agents', 43, 52),
(4, 146, 6552, 'www.trademark-clearinghouse.com/agents', 43, 52),
(4, 146, 6552, 'www.trademark-clearinghouse.com/agents', 43, 52),
(4, 146, 6552, 'www.trademark-clearinghouse.com/agents', 43, 52),
(4, 146, 6552, 'www.trademark-clearinghouse.com/agents', 43, 52),
(4, 146, 6552, 'www.wipo.int/wipo_magazine/en/2012/06/article_0007.html', 43, 87),
(4, 146, 6552, 'www.wipo.int/wipo_magazine/en/2012/06/article_0007.html', 43, 87),
(4, 146, 6552, 'www.wipo.int/wipo_magazine/en/2012/06/article_0007.html', 43, 87),
(4, 146, 6552, 'www.wipo.int/wipo_magazine/en/2012/06/article_0007.html', 43, 87),
(4, 146, 6552, 'www.wipo.int/wipo_magazine/en/2012/06/article_0007.html', 43, 87),
(4, 146, 6552, 'www.wipo.int/wipo_magazine/en/2012/06/article_0007.html', 43, 87),
(4, 146, 6552, 'www.dmoz.org/Business/Transportation_and_Logistics/Freight_Forwarding/Europe/', 42, 87),
(4, 146, 6552, 'www.dmoz.org/Business/Transportation_and_Logistics/Freight_Forwarding/Europe/', 42, 87),
(4, 146, 6552, 'www.dmoz.org/Business/Transportation_and_Logistics/Freight_Forwarding/Europe/', 42, 87),
(4, 146, 6552, 'www.dmoz.org/Business/Transportation_and_Logistics/Freight_Forwarding/Europe/', 42, 87),
(4, 146, 6552, 'www.dmoz.org/Business/Transportation_and_Logistics/Freight_Forwarding/Europe/', 42, 87),
(4, 146, 6552, 'www.dmoz.org/Business/Transportation_and_Logistics/Freight_Forwarding/Europe/', 42, 87),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+to&fr=&fr2=piv-dictionary', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+to&fr=&fr2=piv-dictionary', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+to&fr=&fr2=piv-dictionary', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+to&fr=&fr2=piv-dictionary', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+to&fr=&fr2=piv-dictionary', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+to&fr=&fr2=piv-dictionary', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-shopping', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-shopping', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-shopping', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-shopping', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-shopping', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-shopping', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-answers', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-answers', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-answers', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-answers', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-answers', 41, 93),
(4, 146, 6552, 'tw.autos.search.yahoo.com/search?p=react+on&fr=&fr2=piv-answers', 41, 93),
(4, 146, 6553, 'koen.co.vu/', 83, 51),
(4, 146, 6553, 'koen.co.vu/', 83, 51),
(4, 146, 6553, 'koen.co.vu/', 83, 51),
(4, 146, 6553, 'koen.co.vu/', 83, 51),
(4, 146, 6553, 'koen.co.vu/', 83, 51),
(4, 146, 6553, 'koen.co.vu/', 83, 51),
(4, 146, 6553, 'd.hatena.ne.jp/keyword/Facebook', 82, 93),
(4, 146, 6553, 'd.hatena.ne.jp/keyword/Facebook', 82, 93),
(4, 146, 6553, 'd.hatena.ne.jp/keyword/Facebook', 82, 93),
(4, 146, 6553, 'd.hatena.ne.jp/keyword/Facebook', 82, 93),
(4, 146, 6553, 'd.hatena.ne.jp/keyword/Facebook', 82, 93),
(4, 146, 6553, 'd.hatena.ne.jp/keyword/Facebook', 82, 93),
(4, 146, 6553, 'haneun.co.vu/', 82, 51),
(4, 146, 6553, 'haneun.co.vu/', 82, 51),
(4, 146, 6553, 'haneun.co.vu/', 82, 51),
(4, 146, 6553, 'haneun.co.vu/', 82, 51),
(4, 146, 6553, 'haneun.co.vu/', 82, 51),
(4, 146, 6553, 'haneun.co.vu/', 82, 51),
(4, 146, 6553, 'exclamaras.co.vu/', 82, 51),
(4, 146, 6553, 'exclamaras.co.vu/', 82, 51),
(4, 146, 6553, 'exclamaras.co.vu/', 82, 51),
(4, 146, 6553, 'exclamaras.co.vu/', 82, 51),
(4, 146, 6553, 'exclamaras.co.vu/', 82, 51),
(4, 146, 6553, 'exclamaras.co.vu/', 82, 51),
(4, 146, 6553, 'www.webs.com/', 82, 88),
(4, 146, 6553, 'www.webs.com/', 82, 88),
(4, 146, 6553, 'www.webs.com/', 82, 88),
(4, 146, 6553, 'www.webs.com/', 82, 88),
(4, 146, 6553, 'www.webs.com/', 82, 88),
(4, 146, 6553, 'www.webs.com/', 82, 88),
(4, 146, 6553, 'www.webs.com/Signup?referer=wftr-fr-3-en', 81, 88),
(4, 146, 6553, 'www.webs.com/Signup?referer=wftr-fr-3-en', 81, 88),
(4, 146, 6553, 'www.webs.com/Signup?referer=wftr-fr-3-en', 81, 88),
(4, 146, 6553, 'www.webs.com/Signup?referer=wftr-fr-3-en', 81, 88),
(4, 146, 6553, 'www.webs.com/Signup?referer=wftr-fr-3-en', 81, 88),
(4, 146, 6553, 'www.webs.com/Signup?referer=wftr-fr-3-en', 81, 88),
(4, 146, 6553, 'millionaire-cartel.tumblr.com/', 80, 7),
(4, 146, 6553, 'millionaire-cartel.tumblr.com/', 80, 7),
(4, 146, 6553, 'millionaire-cartel.tumblr.com/', 80, 7),
(4, 146, 6553, 'millionaire-cartel.tumblr.com/', 80, 7),
(4, 146, 6553, 'millionaire-cartel.tumblr.com/', 80, 7),
(4, 146, 6553, 'millionaire-cartel.tumblr.com/', 80, 7),
(4, 146, 6553, 'no-se-quewea-hacer.tumblr.com/', 80, 9),
(4, 146, 6553, 'no-se-quewea-hacer.tumblr.com/', 80, 9),
(4, 146, 6553, 'no-se-quewea-hacer.tumblr.com/', 80, 9),
(4, 146, 6553, 'no-se-quewea-hacer.tumblr.com/', 80, 9),
(4, 146, 6553, 'no-se-quewea-hacer.tumblr.com/', 80, 9),
(4, 146, 6553, 'no-se-quewea-hacer.tumblr.com/', 80, 9),
(4, 146, 6553, 'sexifyy.tumblr.com/', 80, 4),
(4, 146, 6553, 'sexifyy.tumblr.com/', 80, 4),
(4, 146, 6553, 'sexifyy.tumblr.com/', 80, 4),
(4, 146, 6553, 'sexifyy.tumblr.com/', 80, 4),
(4, 146, 6553, 'sexifyy.tumblr.com/', 80, 4),
(4, 146, 6553, 'sexifyy.tumblr.com/', 80, 4),
(4, 146, 6553, 'waitingforastartofall.co.vu/', 79, 51),
(4, 146, 6553, 'waitingforastartofall.co.vu/', 79, 51),
(4, 146, 6553, 'waitingforastartofall.co.vu/', 79, 51),
(4, 146, 6553, 'waitingforastartofall.co.vu/', 79, 51),
(4, 146, 6553, 'waitingforastartofall.co.vu/', 79, 51),
(4, 146, 6553, 'waitingforastartofall.co.vu/', 79, 51);

--
-- Indexes for dumped tables
--

--
-- Indexes for table `campaign`
--
ALTER TABLE `campaign`
  ADD PRIMARY KEY (`campaign_id`);

--
-- Indexes for table `owner`
--
ALTER TABLE `owner`
  ADD PRIMARY KEY (`owner_id`);

--
-- Indexes for table `urls`
--
ALTER TABLE `urls`
  ADD KEY `owner_id` (`owner_id`),
  ADD KEY `campaign_id` (`campaign_id`);

--
-- Constraints for dumped tables
--

--
-- Constraints for table `urls`
--
ALTER TABLE `urls`
  ADD CONSTRAINT `urls_ibfk_1` FOREIGN KEY (`owner_id`) REFERENCES `owner` (`owner_id`),
  ADD CONSTRAINT `urls_ibfk_2` FOREIGN KEY (`campaign_id`) REFERENCES `campaign` (`campaign_id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
