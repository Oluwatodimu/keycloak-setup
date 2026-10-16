<#--
  Greshfi email wrapper: every HTML email is rendered inside this layout.
  The text of each email comes from its "...BodyHtml" key in messages/messages_en.properties.
  The greeting ("Hi {first name},") comes from the emailGreeting key and is skipped if the user has no first name.

  Email apps ignore external CSS, so styles are inline (plus a small <style> block
  for the links and paragraphs that come from the message text).

  Logo: set logoUrl in theme.properties to a PUBLIC https URL of a PNG
  (Gmail/Outlook don't show SVG). Without it, a text wordmark is shown.
-->
<#macro emailLayout>
<!DOCTYPE html>
<html lang="${locale.language}" dir="${(ltr)?then('ltr','rtl')}">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light">
  <meta name="supported-color-schemes" content="light">
  <title>${realmName!""}</title>
  <style>
    .gf-body p { margin: 0 0 16px; }
    .gf-body p:last-child { margin-bottom: 0; }
    .gf-body a { color: #15803d; font-weight: 600; text-decoration: underline; }
    .gf-body b, .gf-body strong { color: #0f1d1a; }
    @media (max-width: 600px) {
      .gf-card { padding: 28px 22px !important; }
    }
  </style>
</head>
<body style="margin:0; padding:0; background:#f5f6fa; -webkit-text-size-adjust:100%;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f5f6fa;">
    <tr>
      <td align="center" style="padding:32px 16px;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:560px;">

          <!-- header -->
          <tr>
            <td align="center" style="background:#0f1d1a; border-radius:14px 14px 0 0; padding:26px 24px;">
              <#if properties.logoUrl?has_content>
                <img src="${properties.logoUrl}" alt="${realmName!"Gresh"}" height="40" style="display:block; height:40px; width:auto; border:0;">
              <#else>
                <span style="font-family:Arial,Helvetica,sans-serif; font-size:26px; font-weight:700; letter-spacing:-0.5px; color:#ffffff;">Gresh<span style="color:#66DD8C;">.</span></span>
              </#if>
            </td>
          </tr>

          <!-- green accent line -->
          <tr><td style="background:#66DD8C; height:4px; line-height:4px; font-size:0;">&nbsp;</td></tr>

          <!-- body: the message text goes here -->
          <tr>
            <td class="gf-card gf-body" style="background:#ffffff; border:1px solid #e5e7eb; border-top:none; border-radius:0 0 14px 14px; padding:36px 40px; font-family:Arial,Helvetica,sans-serif; font-size:15px; line-height:1.6; color:#374151;">
              <#if user?? && user.firstName?has_content>
                <p style="margin:0 0 16px;">${msg("emailGreeting", user.firstName?cap_first)}</p>
              </#if>
              <#nested>
            </td>
          </tr>

          <!-- footer -->
          <tr>
            <td align="center" style="padding:22px 16px 0; font-family:Arial,Helvetica,sans-serif; font-size:12px; line-height:1.6; color:#9ca3af;">
              This email was sent by ${realmName!"Gresh"}. If you didn't expect it, you can safely ignore it.
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>
</#macro>
