export const RecommendationRules = [
  // 🧾 Contract & Tenure
  {
    condition: d => d.contract === 'Month-to-month' && d.tenure < 6,
    message: "Offer 3-month discount for 1-year contract."
  },
  {
    condition: d => d.contract === 'Month-to-month' && d.tenure >= 6,
    message: "Propose annual contract with loyalty benefits."
  },
  {
    condition: d => d.contract === 'One year' && d.tenure < 3,
    message: "Send welcome engagement campaign."
  },
  {
    condition: d => d.contract === 'One year' && d.tenure > 10,
    message: "Offer renewal incentive to lock in early."
  },

  // 🧑 Support Interaction
  {
    condition: d => d.support_calls > 3,
    message: "Schedule retention call: High support frequency."
  },
  {
    condition: d => d.support_calls > 6,
    message: "Escalate to senior retention specialist for personalized offer."
  },
  {
    condition: d => d.avg_support_wait > 5,
    message: "Apologize for service delays and offer compensation credit."
  },

  // 🧑‍💻 Usage & Engagement
  {
    condition: d => d.logins_last_month < 2,
    message: "Trigger re-engagement email campaign."
  },
  {
    condition: d => d.logins_last_month === 0,
    message: "Send win-back push notification and SMS."
  },
  {
    condition: d => d.feature_usage_rate < 0.3,
    message: "Recommend personalized onboarding to increase feature adoption."
  },
  {
    condition: d => d.feature_usage_rate < 0.1,
    message: "Assign success manager for hands-on onboarding."
  },

  // 💰 Billing & Payment Behavior
  {
    condition: d => d.payment_delay_days > 10,
    message: "Send payment reminder with flexible due date."
  },
  {
    condition: d => d.payment_delay_days > 30,
    message: "Offer payment extension plan to avoid churn."
  },
  {
    condition: d => d.autopay === 'No',
    message: "Encourage enabling auto-pay with small discount."
  },

  // 🛡️ Services & Add-ons
  {
    condition: d => d.onlinesecurity === 'No' || d.techsupport === 'No',
    message: "Propose discounted 'Peace of Mind' security bundle."
  },
  {
    condition: d => d.device_protection === 'No',
    message: "Upsell device protection plan."
  },
  {
    condition: d => d.premium_addons === 0,
    message: "Promote add-on packages for enhanced value."
  },

  // 🌐 Internet / Service Quality
  {
    condition: d => d.avg_internet_speed < 25,
    message: "Offer speed upgrade plan with discount."
  },
  {
    condition: d => d.service_outages > 2,
    message: "Provide bill credit for service inconvenience."
  },
  {
    condition: d => d.nps_score < 6,
    message: "Trigger recovery workflow: unhappy customer."
  },

  // 👥 Customer Demographics & Behavior
  {
    condition: d => d.age && d.age < 25 && d.tenure < 6,
    message: "Offer student discount to increase retention."
  },
  {
    condition: d => d.age && d.age > 55 && d.techsupport === 'No',
    message: "Offer assisted setup and training for seniors."
  },
  {
    condition: d => d.region === 'Rural' && d.avg_internet_speed < 20,
    message: "Offer alternative plan or coverage booster."
  },

  // 📊 Risk Indicators
  {
    condition: d => d.risk_score > 0.8,
    message: "Send immediate retention alert to account manager."
  },
  {
    condition: d => d.risk_score > 0.6 && d.contract === 'Month-to-month',
    message: "Offer high-value incentive to lock in annual plan."
  },

  // 🪙 Rewards & Loyalty
  {
    condition: d => d.loyalty_points > 5000,
    message: "Send personalized loyalty rewards to encourage renewal."
  },
  {
    condition: d => d.tenure > 24,
    message: "Offer VIP perks or free upgrade for long-term loyalty."
  }
];
