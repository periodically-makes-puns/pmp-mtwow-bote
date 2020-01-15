import logging
from package.common.utils import data
import sys

import discord
from discord.ext import commands

from package.common.sqlhandle import SQLThread

desc = """A generic miniTWOW Discord bot and website.
Maintainer is currently PMPuns#5728."""
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.INFO)
sql_logger = logging.getLogger("sqlite3")
sql_logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
discord_logger.addHandler(handler)
sql_logger.addHandler(handler)
bot = commands.Bot(command_prefix=data["prefix"], description=desc)
extensions = ["discord.admin"]


sqlthread = SQLThread(data["db"])

@bot.event
async def on_ready():
    if isinstance(data.get("owner"), int):
        discord_logger.debug("Set owner to {:d}".format(data["owner"]))
        bot.owner_id = data["owner"]
    else:
        data["owner"] = (await bot.application_info()).owner.id
    discord_logger.info("Bot is ready!")
    discord_logger.info("Running as {:s} with ID {:d}".format(bot.user.name, bot.user.id))
    for extension in extensions:
        discord_logger.debug("Loading extension {:s}".format(extension))
        try:
            bot.load_extension("package." + extension)
        except commands.ExtensionNotFound:
            discord_logger.debug("Failed to load extension {:s}: Not found.".format(extension))
        except commands.ExtensionAlreadyLoaded:
            discord_logger.debug("Failed to load extension {0:s}: {0:s} was already loaded.".format(extension))
        except commands.ExtensionFailed:
            discord_logger.debug(
                "Failed to load extension {0:s}: {0:s} errored in its entry function.".format(extension))


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    await bot.process_commands(message)


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.NotOwner):
        await ctx.send("You are not the owner >:(", delete_after=5)


@bot.command(brief="Kills the bot.")
@commands.is_owner()
async def kill(ctx: commands.Context):
    discord_logger.info("Received shutdown command from {:s}".format(str(ctx.message.author)))
    await bot.close()
    sys.exit(0)


@bot.command(brief="Loads starting extensions.")
@commands.is_owner()
async def load_default(ctx: commands.Context):
    discord_logger.info("Received load_all command from {:s}".format(str(ctx.message.author)))
    count = 0
    for extension in extensions:
        discord_logger.debug("Loading extension {:s}".format(extension))
        try:
            bot.load_extension("package." + extension)
            count += 1
        except commands.ExtensionNotFound:
            discord_logger.debug("Failed to load extension {:s}: Not found.".format(extension))
        except commands.ExtensionAlreadyLoaded:
            discord_logger.debug("Failed to load extension {0:s}: {0:s} was already loaded.".format(extension))
        except commands.ExtensionFailed:
            discord_logger.debug(
                "Failed to load extension {0:s}: {0:s} errored in its entry function.".format(extension))
    await ctx.send("Loaded {:d} of {:d} extensions. Check debug logs for more details.".format(count, len(extensions)))


@bot.command(brief="Reloads all extensions.")
@commands.is_owner()
async def reload_all(ctx: commands.Context):
    discord_logger.info("Received reload_all command from {:s}".format(str(ctx.message.author)))
    count = 0
    for extension in ctx.bot.cogs:
        discord_logger.debug("Reloading extension {:s}".format(extension))
        try:
            bot.reload_extension("package." + extension)
            count += 1
        except commands.ExtensionNotFound:
            discord_logger.debug("Failed to reload extension {:s}: Not found.".format(extension))
        except commands.ExtensionAlreadyLoaded:
            discord_logger.debug("Failed to reload extension {0:s}: {0:s} was already loaded.".format(extension))
        except commands.ExtensionFailed:
            discord_logger.debug(
                "Failed to reload extension {0:s}: {0:s} errored in its entry function.".format(extension))
    await ctx.send(
        "Reloaded {:d} of {:d} extensions. Check debug logs for more details.".format(count, len(extensions)))


bot.run(data["token"])