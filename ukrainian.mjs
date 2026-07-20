export const TRADE_NOMINATIVE_FORMS = Object.freeze(["угода", "угоди", "угод"]);
export const TRADE_ACCUSATIVE_FORMS = Object.freeze(["угоду", "угоди", "угод"]);
export const CHECK_NOMINATIVE_FORMS = Object.freeze(["перевірка", "перевірки", "перевірок"]);
export const MESSAGE_ACCUSATIVE_FORMS = Object.freeze(["повідомлення", "повідомлення", "повідомлень"]);

const pluralRules = new Intl.PluralRules("uk-UA", { type: "cardinal" });
const formIndexByCategory = Object.freeze({ one: 0, few: 1, many: 2, other: 2 });

export function selectUkCountForm(count, forms) {
  if (!Number.isInteger(count)) throw new TypeError("Ukrainian count must be an integer");
  if (!Array.isArray(forms) || forms.length !== 3) {
    throw new TypeError("Ukrainian count forms must contain [one, few, many]");
  }

  return forms[formIndexByCategory[pluralRules.select(Math.abs(count))]];
}

export function formatUkCount(count, forms) {
  return `${count} ${selectUkCountForm(count, forms)}`;
}

export function agreeUkCount(count, singular, plural) {
  return selectUkCountForm(count, [singular, plural, plural]);
}
