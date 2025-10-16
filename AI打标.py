#AI打标.py
import asyncio
from utils.ai_tagger import AITagger
from utils.config_handler import ConfigHandler

async def main():
    config_handler = ConfigHandler(period='ai_tagger')
    dates = ConfigHandler.get_daily_dates() 
    input_file = config_handler.get_path('new_song_diff', 'input_paths', **dates)
    output_file = config_handler.get_path('tagged_output', 'output_paths', new_date=dates['new_date'])
    tagger = AITagger(input_file=input_file, output_file=output_file, config_handler=config_handler)
    await tagger.run()

if __name__ == "__main__":
    asyncio.run(main())

