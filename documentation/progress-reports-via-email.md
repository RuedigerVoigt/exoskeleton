# Progress Reports via Email

Exoskeleton can send email
* when it starts,
* when it reaches a milestone
* after it stops.


## Technical Prerequisites

Note, that it usually does not work to send email from a system with a dynamic IP address (typically your home computer) as most mail servers will classify them as spam.

Even if you send from a machine with static IP, many things might go wrong. For example, there might be a [SPF setting](https://en.wikipedia.org/wiki/Sender_Policy_Framework "Wikipedia explaining the Sender Policy Framework") for the sending domain that limits sending mails to a server specified in the MX record of your DNS entry.

However, exoskeleton can connect to a regular mail server and send mails via that.

## Activating Mails


When you create your bot, you can set the parameter `mail_settings` which expects a dictionary. The following keys are supported:

* **server** (default: 'localhost'): either your local machine or a remote server
* **server_port** (default: None)
* **encryption** (default: 'off')
* **username** (default: None)
* **passphrase** (default: None)
* **recipient** (default: None): email address of the recipient
* **sender** (default: None): email address of the sender


Obviously `mail_admin` and `mail_sender` are required to send email.

If `mail_settings` are given, the parameter `mail_behavior` (must be a dictionary) determines when to send mail. The following keys are supported:

* **send_start_msg** (bool / default: True): whether to send a start message
* **send_finish_msg** (bool / default: False): send a message when the bot finishes (requires that is set up to finish once the queue is empty).
* **milestone_num** (int / default: None): Send a mail every time a certain number of tasks has been handled. Setting it to 1000 means, that you get an email after each 1,000 queue items are handled.

`send_start_msg` defaults to True, so the bot will right away send an email. This is a good way to check whether sending a mail does work. If the receiving mail server uses [greylisting](https://en.wikipedia.org/wiki/Greylisting "Wikipedia on this method to reduce spam by introducing wait time for unknown senders"), it may initially take some minutes to get this email.

> :arrow_right: **[Work with versions and labels](versions-and-labels.md)**