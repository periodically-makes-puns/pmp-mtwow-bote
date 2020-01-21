from logging import getLogger
from secrets import token_hex
from time import strftime, gmtime
import sqlite3

import discord
from discord.ext import commands

from ..common.sqlhandle import SQLThread
from ..common.sqlutils import *
from ..common.utils import sqlthread, name_string, parse_time, format_time

discord_logger = getLogger('discord')


class Database(commands.Cog):
    """A Cog that provides SQL utility methods for an Administrator."""
    def __init__(self, bot: commands.Bot, sql: SQLThread):
        commands.Cog.__init__(self)
        self.sql = sql
        self.bot = bot

    @commands.command(brief="Constructs the SQLite tables.", help="Constructs the SQLite schema. No arguments.")
    @commands.is_owner()
    async def construct(self, ctx: commands.Context):
        res = construct_schema(self.sql)
        if isinstance(res, sqlite3.Error):
            raise res
        await ctx.send("Finished constructing schema without issue.")

    @commands.command(brief="Destroys the SQLite tables.", help="Destroys the SQLite schema.")
    @commands.is_owner()
    async def destroy(self, ctx: commands.Context):
        res = destroy_schema(self.sql)
        if isinstance(res, sqlite3.Error):
            raise res
        await ctx.send("Finished destroying schema without issue.")

    @commands.command(brief="Backs up the SQLite tables to a file.", help="Takes argument filename. Automatically " +
        "appends datetime data and file extension, with token to prevent collisions.")
    @commands.is_owner()
    async def backup(self, ctx: commands.Context, filename: str = "backup"):
        filename += strftime("%Y-%m-%d-%H-%M-%S") + "-" + token_hex(16) + ".sqlite"
        filename = "backups/" + filename
        await ctx.send("Saved to filename: {}".format(filename))
        res = snapshot_schema(self.sql, filename)
        if isinstance(res, sqlite3.Error):
            raise res
        await ctx.send("Finished snapshotting schema without issue.")

    @commands.command(brief="Make a SQL request and get a result (if any).", help="Greedily takes string for request."
                      + "Quote the query string and put it last. Any params should go first.")
    @commands.is_owner()
    async def get(self, ctx: commands.Context, *request: str):
        res = get(self.sql, request[-1], "".join(request[0:-1]))
        if isinstance(res, sqlite3.Error):
            raise res
        await ctx.send("Result: ``" + str(res) + "``")

    @commands.command(brief="Make a SQL request and throw errors.", help="Greedily takes string for request."
                      + "Quote the query string and put it last. Any params should go first.")
    @commands.is_owner()
    async def run(self, ctx: commands.Context, *request: str):
        try:
            res = run(self.sql, request[-1], "".join(request[0:-1]))
        except Exception as e:
            await ctx.send("Error: ```\n" + str(e) + "```")
            return
        await ctx.send("Result: ``" + str(res) + "``")

    @commands.command(brief="Get the current status.")
    async def status(self, ctx: commands.Context):
        status = list(get_status(self.sql))[0]
        embed = discord.Embed(color=0x3daeff)
        embed.title = "Current mTWOW Status"
        embed.set_author(name=self.bot.user.name) \
            .add_field(name="Round Number", value=str(status.round_num)) \
            .add_field(name="Prompt", value=status.prompt) \
            .add_field(name="Phase", value=status.phase) \
            .add_field(name="Time Left", value=format_dhms(status.deadline)) \
            .add_field(name="Deadline of Current Phase", value=format_time(status.start_time + status.deadline)) \
            .set_footer(text=name_string(self.bot.get_user(self.bot.owner_id)))
        await ctx.send(embed=embed)

    @commands.command(brief="Set the deadline.")
    @commands.is_owner()
    async def set_deadline(self, ctx: commands.Context, deadline: parse_time):
        ctime = time_ns() // 1000000
        set_time(self.sql, ctime, deadline)
        await ctx.send("Set deadline to {}".format(format_time(ctime + deadline)))

    @commands.command(brief="Update the deadline timer.")
    @commands.is_owner()
    async def update_time(self, ctx: commands.Context):
        update_timers(self.sql)
        await ctx.send("Done.")


def setup(bot: commands.Bot):
    bot.add_cog(Database(bot, sqlthread))
    discord_logger.info("Loaded extension discord.sqlutils")


def teardown(bot: commands.Bot):
    bot.remove_cog("Database")
    discord_logger.info("Unloaded extension discord.sqlutils")