const storageKey = "homepage-suggested-book";

export const suggestedBooks = [
  { book: "RUT", name: "Ruth" },
  { book: "GEN", name: "Genesis" },
  { book: "JON", name: "Jonah" },
  { book: "MAT", name: "Matthew", chapter: 7 },
  { book: "EXO", name: "Exodus" },
  { book: "LUK", name: "Luke" },
  { book: "JOS", name: "Joshua", chapter: 3, verse: 16 },
];

export function nextSuggestedBook(previousBook) {
  const index = suggestedBooks.findIndex((item) => item.book === previousBook);
  return suggestedBooks[(index + 1) % suggestedBooks.length];
}

export function loadSuggestedBook() {
  try {
    return nextSuggestedBook(localStorage.getItem(storageKey));
  } catch (error) {
    console.warn("Could not restore the previous homepage suggestion.", error);
    return nextSuggestedBook(null);
  }
}

export function saveSuggestedBook(book) {
  try {
    localStorage.setItem(storageKey, book);
  } catch (error) {
    console.warn("Could not save the homepage suggestion.", error);
  }
}
