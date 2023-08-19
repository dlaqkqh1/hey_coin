import discord
import environment
import consumer_thread
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


@bot.command()
async def start(ctx):
    ct = consumer_thread.ConsumerTread()
    thread1, thread2, consumer1, consumer2 = ct.start_consuming()

    try:
        await ctx.send(thread1.join())
        await ctx.send(thread2.join())

    except KeyboardInterrupt:
        pass

    finally:
        consumer1.close()
        consumer2.close()

bot.run(TOKEN)
