 #!/usr/bin/python
 # -*- coding: utf-8 -*-

import sys
import os
import socket
import requests
import re
import urlparse
from time import time
from lxml import html
from lxml import etree
from contextlib import closing

from util import is_doi_url
from util import elapsed


DEBUG_SCRAPING = False





class ArticlePage(object):
    def __init__(self, doi):
        self.pdf_url = None
        self.resolved_url = None
        self.license = "unknown"
        self.error = None
        self.error_message = None
        self.pdf_is_free_to_read = None

        self.doi = doi



    def scrape_for_fulltext_link(self):

        doi_resolver_url = u"http://doi.org/{}".format(self.doi)

        try:
            with closing(http_get(doi_resolver_url)) as r:

                # get the HTML tree
                page = r.content

                # the DOI resolved to some URL; it's handy to note it down for debugging.
                self.resolved_url = r.url

                # set the license if we can find one
                scraped_license = find_normalized_license(page)
                if scraped_license:
                    self.license = scraped_license

                pdf_download_link = find_pdf_link(page, self.doi)
                if pdf_download_link is not None:
                    if DEBUG_SCRAPING:
                        print u"found a PDF download link: {} {} [{}]".format(
                            pdf_download_link.href, pdf_download_link.anchor, self.doi)

                    self.pdf_url = get_link_target(pdf_download_link, r.url)


                    # let's follow the pdf link to see if it leads a a real pdf, or some paywall nonsense
                    if DEBUG_SCRAPING:
                        print u"checking to see the PDF link actually gets a PDF [{}]".format(self.doi)

                    if gets_a_pdf(pdf_download_link, r.url, self.doi):
                        self.pdf_is_free_to_read = True
                    else:
                        self.pdf_is_free_to_read = False

        except requests.exceptions.ConnectionError:
            print u"ERROR: connection error on {} in scrape_for_fulltext_link, skipping.".format(self.doi)

        except requests.Timeout:
            print u"ERROR: timeout error on {} in scrape_for_fulltext_link, skipping.".format(self.doi)

        except requests.exceptions.InvalidSchema:
            print u"ERROR: InvalidSchema error on {} in scrape_for_fulltext_link, skipping.".format(self.doi)

        except requests.exceptions.RequestException as e:
            print u"ERROR: RequestException error on {} in scrape_for_fulltext_link, skipping.".format(self.doi)

        if DEBUG_SCRAPING:
            print u"found no PDF download link.  end of the line. [{}]".format(self.doi)

        return self

    def __repr__(self):
        return u"<{} ({}) {}>".format(self.__class__.__name__, self.doi)

    def to_dict(self):
        return {
            "doi": self.doi,
            "resolved_url": self.resolved_url,
            "pdf_url": self.pdf_url,
            "pdf_is_free_to_read": self.pdf_is_free_to_read,
            "license": self.license,
            "error": self.error,
            "error_message": self.error_message
        }



def get_tree(page):
    page = page.replace("&nbsp;", " ")  # otherwise starts-with for lxml doesn't work
    try:
        tree = html.fromstring(page)
    except etree.XMLSyntaxError:
        print u"XMLSyntaxError in get_tree; not parsing."
        tree = None

    return tree




# = open journal http://www.emeraldinsight.com/doi/full/10.1108/00251740510597707
# = closed journal http://www.emeraldinsight.com/doi/abs/10.1108/14777261111143545


def gets_a_pdf(link, base_url, doi=None):

    if is_purchase_link(link):
        return False

    absolute_url = get_link_target(link, base_url)
    if DEBUG_SCRAPING:
        print u"checking to see if {} is a pdf".format(absolute_url)

    start = time()
    try:
        with closing(http_get(absolute_url, stream=True, read_timeout=10)) as r:
            if resp_is_pdf_from_header(r):
                if DEBUG_SCRAPING:
                    print u"http header says this is a PDF. took {}s {}".format(
                        elapsed(start), absolute_url)
                return True

            # everything below here needs to look at the content
            # so bail here if the page is too big
            if is_response_too_large(r):
                if DEBUG_SCRAPING:
                    print u"response is too big for more checks in gets_a_pdf"
                return False

            # some publishers send a pdf back wrapped in an HTML page using frames.
            # this is where we detect that, using each publisher's idiosyncratic templates.
            # we only check based on a whitelist of publishers, because downloading this whole
            # page (r.content) is expensive to do for everyone.
            if 'onlinelibrary.wiley.com' in absolute_url:
                # = closed journal http://doi.org/10.1111/ele.12585
                # = open journal http://doi.org/10.1111/ele.12587 cc-by
                if '<iframe' in r.content:
                    if DEBUG_SCRAPING:
                        print u"this is a Wiley 'enhanced PDF' page. took {}s [{}]".format(
                            elapsed(start), absolute_url)
                    return True

            elif 'ieeexplore' in absolute_url:
                # (this is a good example of one dissem.in misses)
                # = open journal http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=6740844
                # = closed journal http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=6045214
                if '<frame' in r.content:
                    if DEBUG_SCRAPING:
                        print u"this is a IEEE 'enhanced PDF' page. took {}s [{}]".format(
                                    elapsed(start), absolute_url)
                    return True

            elif 'sciencedirect' in absolute_url:
                if u"does not support the use of the crawler software" in r.content:
                    return True


        if DEBUG_SCRAPING:
            print u"we've decided this ain't a PDF. took {}s [{}]".format(
                elapsed(start), absolute_url)
        return False
    except requests.exceptions.ConnectionError:
        print u"ERROR: connection error in gets_a_pdf, skipping."
        return False
    except requests.Timeout:
        print u"ERROR: timeout error in gets_a_pdf, skipping."
        return False
    except requests.exceptions.InvalidSchema:
        print u"ERROR: InvalidSchema error in gets_a_pdf, skipping."
        return False
    except requests.exceptions.RequestException:
        print u"ERROR: RequestException error in gets_a_pdf, skipping."
        return False


# it matters this is just using the header, because we call it even if the content
# is too large.  if we start looking in content, need to break the pieces apart.
def resp_is_pdf_from_header(resp):
    looks_good = False

    for k, v in resp.headers.iteritems():
        if v:
            key = k.lower()
            val = v.lower()

            if key == "content-type" and "application/pdf" in val:
                looks_good = True

            if key =='content-disposition' and "pdf" in val:
                looks_good = True

    return looks_good


class DuckLink(object):
    def __init__(self, href, anchor):
        self.href = href
        self.anchor = anchor


def get_useful_links(tree):
    ret = []
    if tree is None:
        return ret

    # remove related content sections
    # gets rid of these bad links: http://www.tandfonline.com/doi/abs/10.4161/auto.19496
    for related_content in tree.xpath("//div[@class=\'relatedItem\']"):
        # tree.getparent().remove(related_content)
        related_content.clear()

    # now get the links
    links = tree.xpath("//a")

    for link in links:
        link_text = link.text_content().strip().lower()
        if link_text:
            link.anchor = link_text
            if "href" in link.attrib:
                link.href = link.attrib["href"]

        else:
            # also a useful link if it has a solo image in it, and that image includes "pdf" in its filename
            link_content_elements = [l for l in link]
            if len(link_content_elements)==1:
                link_insides = link_content_elements[0]
                if link_insides.tag=="img":
                    if "src" in link_insides.attrib and "pdf" in link_insides.attrib["src"]:
                        link.anchor = u"image: {}".format(link_insides.attrib["src"])
                        if "href" in link.attrib:
                            link.href = link.attrib["href"]

        if hasattr(link, "anchor") and hasattr(link, "href"):
            ret.append(link)

    return ret


def is_purchase_link(link):
    # = closed journal http://www.sciencedirect.com/science/article/pii/S0147651300920050
    if "purchase" in link.anchor:
        print u"found a purchase link!", link.anchor, link.href
        return True

    return False

def has_bad_href_word(href):
    href_blacklist = [
        # = closed 10.1021/acs.jafc.6b02480
        # editorial and advisory board
        "/eab/",

        # = closed 10.1021/acs.jafc.6b02480
        "/suppl_file/",

        # https://lirias.kuleuven.be/handle/123456789/372010
        "supplementary+file",

        # http://www.jstor.org/action/showSubscriptions
        "showsubscriptions",

        # 10.7763/ijiet.2014.v4.396
        "/faq",

        # 10.1515/fabl.1988.29.1.21
        "{{",

        # 10.2174/1389450116666150126111055
        "cdt-flyer",

        # 10.1111/fpa.12048
        "figures",

        # prescribing information, see http://www.nejm.org/doi/ref/10.1056/NEJMoa1509388#t=references
        "janssenmd.com",

        # prescribing information, see http://www.nejm.org/doi/ref/10.1056/NEJMoa1509388#t=references
        "community-register",

        # prescribing information, see http://www.nejm.org/doi/ref/10.1056/NEJMoa1509388#t=references
        "quickreference",

        # 10.4158/ep.14.4.458
        "libraryrequestform",

        # http://www.nature.com/nutd/journal/v6/n7/full/nutd201620a.html
        "iporeport",

        #https://ora.ox.ac.uk/objects/uuid:06829078-f55c-4b8e-8a34-f60489041e2a
        "no_local_copy"
    ]
    for bad_word in href_blacklist:
        if bad_word in href.lower():
            return True
    return False


def has_bad_anchor_word(anchor_text):
    anchor_blacklist = [
        # = closed repo https://works.bepress.com/ethan_white/27/
        "user",
        "guide",

        # = closed 10.1038/ncb3399
        "checklist",

        # https://hal.archives-ouvertes.fr/hal-00085700
        "metadata from the pdf file",
        u"récupérer les métadonnées à partir d'un fichier pdf",

        # = closed http://europepmc.org/abstract/med/18998885
        "bulk downloads",

        # = closed 10.1021/acs.jafc.6b02480
        "masthead",

        # closed http://eprints.soton.ac.uk/342694/
        "download statistics",

        # no examples for these yet
        "supplement",
        "figure",
        "faq"
    ]
    for bad_word in anchor_blacklist:
        if bad_word in anchor_text.lower():
            return True

    return False


# url just used for debugging
def find_pdf_link(page, url):

    # tests we are not sure we want to run yet:
    # if it has some semantic stuff in html head that says where the pdf is: that's the pdf.
    # = open http://onlinelibrary.wiley.com/doi/10.1111/tpj.12616/abstract


    # DON'T DO THESE THINGS:
    # search for links with an href that has "pdf" in it because it breaks this:
    # = closed journal http://onlinelibrary.wiley.com/doi/10.1162/10881980152830079/abstract


    tree = get_tree(page)
    if tree is None:
        return None

    # before looking in links, look in meta for the pdf link
    # = open journal http://onlinelibrary.wiley.com/doi/10.1111/j.1461-0248.2011.01645.x/abstract
    # = open journal http://doi.org/10.1002/meet.2011.14504801327
    # = open repo http://hdl.handle.net/10088/17542
    # = open http://handle.unsw.edu.au/1959.4/unsworks_38708 cc-by

    if "citation_pdf_url" in page:
        metas = tree.xpath("//meta")
        for meta in metas:
            if "name" in meta.attrib and meta.attrib["name"]=="citation_pdf_url":
                if "content" in meta.attrib:
                    link = DuckLink(href=meta.attrib["content"], anchor="<meta citation_pdf_url>")
                    return link

    # (this is a good example of one dissem.in misses)
    # = open journal http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=6740844
    # = closed journal http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=6045214
    if '"isOpenAccess":true' in page:
        # this is the free fulltext link
        article_number = url.rsplit("=", 1)[1]
        href = "http://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber={}".format(article_number)
        link = DuckLink(href=href, anchor="<ieee isOpenAccess>")
        return link

    for link in get_useful_links(tree):

        # there are some links that are SURELY NOT the pdf for this article
        if has_bad_anchor_word(link.anchor):
            continue

        # there are some links that are SURELY NOT the pdf for this article
        if has_bad_href_word(link.href):
            continue


        # download link ANCHOR text is something like "manuscript.pdf" or like "PDF (1 MB)"
        # = open repo http://hdl.handle.net/1893/372
        # = open repo https://research-repository.st-andrews.ac.uk/handle/10023/7421
        # = open repo http://dro.dur.ac.uk/1241/
        if "pdf" in link.anchor:
            return link


        # button says download
        # = open repo https://works.bepress.com/ethan_white/45/
        # = open repo http://ro.uow.edu.au/aiimpapers/269/
        # = open repo http://eprints.whiterose.ac.uk/77866/
        if "download" in link.anchor:
            if "citation" in link.anchor:
                pass
            else:
                return link

        # download link is identified with an image
        for img in link.findall("img"):
            try:
                if "pdf" in img.attrib["src"].lower():
                    return link
            except KeyError:
                pass  # no src attr

        try:
            if "pdf" in link.attrib["title"].lower():
                return link
        except KeyError:
            pass



    return None



def get_link_target(link, base_url):
    try:
        url = link.href
    except KeyError:
        return None

    url = re.sub(ur";jsessionid=\w+", "", url)
    if base_url:
        url = urlparse.urljoin(base_url, url)

    return url




def find_normalized_license(text):
    normalized_text = text.replace(" ", "").replace("-", "").lower()

    # the lookup order matters
    # assumes no spaces, no dashes, and all lowercase
    # inspired by https://github.com/CottageLabs/blackbox/blob/fc13e5855bd13137cf1ef8f5e93883234fdab464/service/licences.py
    # thanks CottageLabs!  :)

    license_lookups = [
        ("creativecommons.org/licenses/byncnd", "cc-by-nc-nd"),
        ("creativecommonsattributionnoncommercialnoderiv", "cc-by-nc-nd"),
        ("ccbyncnd", "cc-by-nc-nd"),

        ("creativecommons.org/licenses/byncsa", "cc-by-nc-sa"),
        ("creativecommonsattributionnoncommercialsharealike", "cc-by-nc-sa"),
        ("ccbyncsa", "cc-by-nc-sa"),

        ("creativecommons.org/licenses/bynd", "cc-by-nd"),
        ("creativecommonsattributionnoderiv", "cc-by-nd"),
        ("ccbynd", "cc-by-nd"),

        ("creativecommons.org/licenses/bysa", "cc-by-sa"),
        ("creativecommonsattributionsharealike", "cc-by-sa"),
        ("ccbysa", "cc-by-sa"),

        ("creativecommons.org/licenses/bync", "cc-by-nc"),
        ("creativecommonsattributionnoncommercial", "cc-by-nc"),
        ("ccbync", "cc-by-nc"),

        ("creativecommons.org/licenses/by", "cc-by"),
        ("creativecommonsattribution", "cc-by"),
        ("ccby", "cc-by"),

        ("creativecommons.org/publicdomain/zero", "cc0"),
        ("creativecommonszero", "cc0"),

        ("creativecommons.org/publicdomain/mark", "pd"),
        ("publicdomain", "pd"),

        # ("openaccess", "oa")
    ]

    for (lookup, license) in license_lookups:
        if lookup in normalized_text:
            return license
    return "unknown"



def is_response_too_large(r):
    if not "Content-Length" in r.headers:
        print u"can't tell if page is too large, no Content-Length header {}".format(r.url)
        return False

    content_length = r.headers["Content-Length"]
    # if is bigger than 1 MB, don't keep it don't parse it, act like we couldn't get it
    # if doing 100 in parallel, this would be 100MB, which fits within 512MB dyno limit
    if int(content_length) >= (1 * 1000 * 1000):
        print u"Content Too Large on GET on {url}".format(url=r.url)
        return True
    return False

# this is mostly copied from oaDOI but without the cache stuff.
def http_get(url, headers={}, read_timeout=20, stream=False, allow_redirects=True):

    try:
        try:
            print u"LIVE GET on {url}".format(url=url)
        except UnicodeDecodeError:
            print u"LIVE GET on an url that throws UnicodeDecodeError"

        connect_timeout = 3
        r = requests.get(url,
                         headers=headers,
                         timeout=(connect_timeout, read_timeout),
                         stream=stream,
                         allow_redirects=allow_redirects,
                         verify=False)

        if r and not r.encoding:
            r.encoding = "utf-8"

    except (requests.exceptions.Timeout, socket.timeout) as e:
        print u"timed out on GET on {url}".format(url=url)
        raise

    except requests.exceptions.RequestException as e:
        print u"RequestException on GET on {url}".format(url=url)
        raise

    return r
