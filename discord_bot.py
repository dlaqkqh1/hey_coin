import discord
from discord.ext import commands

TOKEN = "MTEzMTQ1MzkxNTY3NDcyNjQzMQ.G4gRcs.nLFP1hiifMAYoEXGU7XtBb0o6Ubup0FQmQAey4"

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
