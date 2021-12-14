import aiohttp
import aiofiles
from asyncio import gather, get_event_loop
import os
import lzma
import zstandard
import bz2
import io
import json
from tqdm.asyncio import tqdm as asyn_tqdm
from tqdm import tqdm

URL = "http://files.pushshift.io/reddit/submissions/"
BASE_FILE_NAME = "RS_{}-{}{}"
EXTENSIONS = [".zst", ".xz", ".bz2"]
# YEARS = ["2015", "2016", "2017", "2018", "2019", "2020"]
# YEARS = ["2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014"]
YEARS = ["2021"]
OUTPUT = "data"

SUBREDDITS_TO_KEEP = [
    "7cupsoftea",
    "abuse",
    "adultsurvivors",
    "addiction",
    "alcoholism",
    "afterthesilence",
    "agoraphobia",
    "anger",
    "anxiety",
    "backonyourfeet",
    "bipolarreddit",
    "bipolarsos",
    "bullying",
    "bpd",
    "calmhands",
    "compulsiveskinpicking",
    "cptsd",
    "depersonalization",
    "depression",
    "depressed",
    "domesticviolence",
    "dp_dr",
    "dpdr",
    "eatingdisorders",
    "emotionalabuse",
    "existential_crisis",
    "feelgood",
    "foreveralone",
    "getting_over_it",
    "gfd",
    "griefsupport",
    "hardshipmates",
    "helpmecope",
    "heretohelp",
    "itgetsbetter",
    "lonely",
    "lostalovedone",
    "offmychest",
    "makemefeelbetter",
    "maladaptivedreaming",
    "mentalhealth",
    "miscarriage",
    "mmfb",
    "radical_mental_health",
    "reasonstolive",
    "ocd",
    "panicparty",
    "psychoticreddit",
    "psychosis",
    "ptsd",
    "ptsdcombat",
    "rapecounseling",
    "schizophrenia",
    "socialanxiety",
    "stopselfharm",
    "suicidewatch",
    "suicidebereavement",
    "survivorsofabuse",
    "survivorsunited",
    "therapy",
    "traumatoolbox",
    "trichsters"
]

OPEN_FUNCS = {
    '.xz': lzma.open,
    '.bz2': bz2.open,
    '.zst': zstandard.open,
}


async def download_file(session: aiohttp.ClientSession, year, file_num):
    for ext in EXTENSIONS:
        url_file = BASE_FILE_NAME.format(year, str(file_num + 1).zfill(2), ext)
        file_name = url_file.split('/')[-1].split('.')[0] + ext
        if not os.path.isfile(os.path.join(OUTPUT, file_name)):
            response = await session.request(method="GET", url=URL + url_file)
            if response.status == 200:
                print("Starting file {}".format(file_name))
                async with aiofiles.open(os.path.join(OUTPUT, file_name), "ba") as f:
                    async for data in asyn_tqdm(response.content.iter_chunked(100 * 1024), miniters=500):
                        await f.write(data)
                print("Downloaded {}".format(file_name))
                return file_name, ext
        else:
            print("File already downloaded {}".format(file_name))
            return file_name, ext


def open_file(file_name, ext):
    return OPEN_FUNCS[ext](os.path.join(OUTPUT, file_name), dctx=zstandard.ZstdDecompressor(max_window_size=2147483648))


async def process_file(session, year, file_num):
    file_name, ext = await download_file(session, year, file_num)
    with open_file(file_name, ext) as binary_reader:
        output_file = file_name.split(".")[0] + "_p.json"
        # async with aiofiles.open(os.path.join(OUTPUT, output_file), 'w') as output_write:
        with open(os.path.join(OUTPUT, output_file), 'w', encoding='utf-8') as output_write:
            f = io.TextIOWrapper(binary_reader, encoding='utf-8')
            for line in tqdm(f, miniters=4000):
                try:
                    json_data = json.loads(line)
                    process_reddit_data(output_write, json_data)
                except:
                    print(line)
    print("finished {}".format(file_name))
    os.remove(os.path.join(OUTPUT, file_name))


def process_reddit_data(output_write, json_data):
    if "subreddit" in json_data and json_data["subreddit"].lower() in SUBREDDITS_TO_KEEP:
        data = [json_data["id"],
                json_data["created_utc"],
                json_data["author"],
                json_data["subreddit"],
                json_data["title"],
                json_data["selftext"]]
        # await output_write.write(data)
        json.dump(data, output_write, ensure_ascii=False, indent=4)


async def main():
    for year in YEARS:
        for file_num in range(12):
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5000)) as session:
                await gather(*[process_file(session, year, file_num)])


if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main())
