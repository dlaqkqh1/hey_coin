import discord
import environment
from discord.ext import commands

TOKEN = environment.DISCORD_BOT_TOKEN

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix='!', intents=intents)


@bot.event
async def on_ready():
    print(f'Login bot: {bot.user}')


@bot.command()
async def hello(ctx):
    await ctx.send("hello")

bot.run(TOKEN)
