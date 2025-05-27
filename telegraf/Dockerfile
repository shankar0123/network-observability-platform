FROM telegraf:1.29 # Use a recent stable version

# Copy the Telegraf configuration file into the container
COPY telegraf.conf /etc/telegraf/telegraf.conf

# Expose Telegraf's default port if needed for any internal stats/API, though not strictly necessary for this setup
# EXPOSE 8094

CMD ["telegraf"]
