from discord.ext import commands
import logging

discord_logger = logging.getLogger('discord')


class Administrator(commands.Cog):
    @commands.command(brief="Reloads a module.")
    @commands.is_owner()
    async def reload(self, ctx: commands.Context, module: str):
        discord_logger.info("{:s} issued command to reload module {:s}".format(str(ctx.message.author), module))
        try:
            ctx.bot.reload_extension("package." + module)
            await ctx.send("Reloaded extension {:s}.".format(module))
        except commands.ExtensionNotFound:
            await ctx.send("Failed to reload extension {:s}: Not found.".format(module))
        except commands.ExtensionFailed:
            await ctx.send("Failed to reload extension {:s}: Failed to set up.".format(module))
        except commands.ExtensionNotLoaded:
            await ctx.send(
                "Failed to reload extension {:s}: Not loaded yet. Please use load command to load extension first.".format(
                    module))

    @commands.command(brief="Unloads a module.")
    @commands.check(commands.is_owner())
    async def unload(self, ctx: commands.Context, module: str):
        discord_logger.info("{:s} issued command to unload module {:s}".format(str(ctx.message.author), module))
        try:
            ctx.bot.unload_extension("package." + module)
        except commands.ExtensionNotLoaded:
            await ctx.send(
                "Failed to unload extension {:s}: Not loaded yet. Please use load command to load extension first.".format(
                    module))

    @commands.command(brief="Loads a module.")
    @commands.is_owner()
    async def load(self, ctx: commands.Context, module: str):
        discord_logger.info("{:s} issued command to load module {:s}".format(str(ctx.message.author), module))
        try:
            ctx.bot.load_extension(module)
        except commands.ExtensionNotFound:
            await ctx.send("Failed to load extension {:s}: Not found.".format(module))
        except commands.ExtensionAlreadyLoaded:
            await ctx.send("Failed to load extension {0:s}: {0:s} was already loaded.".format(module))
        except commands.ExtensionFailed:
            await ctx.send("Failed to load extension {0:s}: {0:s} errored in its entry function.".format(module))


def setup(bot: commands.Bot):
    bot.add_cog(Administrator())
    discord_logger.info("Loaded extension generic.admin")


def teardown(bot: commands.Bot):
    bot.remove_cog("Administrator")
    discord_logger.info("Unloaded extension generic.admin")